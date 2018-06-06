#!/usr/bin/python
"""
BEGIN ./failed_jobs.py INFO
The script finds failed metadadata jobs in this confluence page:
https://confluence.arijlopez.com/display/OPS/Client+Metadata+status
Then logs in in dqm and re run the failed jobs
Example cron to run the script, depending on how many times needed:
0 8,11,14 * * * /projects/scripts/failed_jobs.py >> /var/log/failed_jobs.log
END failed_jobs.py INFO
Author: Ari Lopez
"""
import requests
import re
import mechanize
import ymlconfig
import time
from requests.auth import HTTPBasicAuth
import cookielib
import yaml


config_path = "/etc/dqm/config.yml"
cfg = ymlconfig.load_file(config_path)
"""
confluence:
    user: XXX
    password: YYY
dqm:
    user: AAA
    password: BBB
"""
"""
 login page for dqm
"""
dqm_url = "http://dqm.arijlopez.com/login"


def today():
    now = time.strftime("%c")
    return str(now)


"""
 try to connect to a website at least 3 times if any error
 return the response
"""


def url_response(url):
    counter = 0
    while counter < 3:
        try:
            req = urllib2.Request(url)
            res = urllib2.urlopen(req)
            time.sleep(30)
        except Exception as ex:
            print(
                "%s: Something went wrong trying to access to confluence: %s" %
                (today(), ex))
            counter += 1
            if counter > 2:
                exit()
            continue
        break
    return res


"""
try to read the response of a website at least 3 times http response code not 200
return the website text
"""


def read_website(url):
    print("INFO: Reading Confluence")
    try:
        req = requests.get(
            url,
            auth=HTTPBasicAuth(
                cfg.confluence.user,
                cfg.confluence.password
            )
        )
        print("DEBUG: loaded confluence")
    except requests.exceptions.ConnectionError:
        print(
            "ERROR: A connection error occurred when connecting to confluence"
        )
        raise SystemExit(1)
    if req.status_code != 200:
        read_website(url)
    return req.text


"""
filters the conflunce metadata status page to find the jobs in error
returns the metadata jobs in error
"""


def metadata_jobs_in_error(data):
    line = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    urls = re.findall(line, data)
    metadata_string = 'http://dqm.arijlopez.com/mgmt/service/job/startMetadataJob'
    met_jobs_error = []
    for url in urls:
        if metadata_string in url:
            met_jobs_error.append(url)
    if not met_jobs_error:
        print("%s : There are not metadata jobs to rerun" % today())
        raise SystemExit(0)
    return met_jobs_error


"""
log in website a keep the cookie in order to hit other url of the same website
returns the browser logged in the wanted website
"""


def login_website(url, username, password):
    # Browser
    br = mechanize.Browser()

    # Enable cookie support for urllib2
    cookiejar = cookielib.LWPCookieJar()
    br.set_cookiejar(cookiejar)

    # Broser options
    br.set_handle_equiv(True)
    br.set_handle_gzip(True)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.set_handle_robots(False)

    br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)
    br.addheaders = [
        ('User-agent',
         'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
    # open url
    br.open(url)
    # find hidden forms
    br.select_form(name="loginForm")
    # add username and password forms
    br["loginForm:username"] = username
    br["loginForm:password"] = password
    br.submit()
    return br


def main():
    # conflunce client metadata status page with username and password
    scheme = "https://"
    url = "confluence.arijlopez.com"
    path = "/display/OPS/Client+Metadata+status"
    metadata_confluence_url = scheme + url + path
    # find the failed jobs
    metadata_site = read_website(metadata_confluence_url)
    jobs = metadata_jobs_in_error(metadata_site)
    # login dqm
    dqm = login_website(dqm_url, cfg.dqm.user, cfg.dqm.password)
    # re run metadata jobs
    for job in jobs:
        page = dqm.open(job)
        return_page = page.read()
        if return_page is None or not return_page:
            print("%s : Response for %s is empty" % (today(), job))
            continue
        attempt = 0
        while True:
            if '\"responseStatus\":\"OK\"' in return_page:
                print("""
%s: Metadata job for client: %s has been triggered successfully""" % (
                    today(), job))
                attempt = 0
                time.sleep(2)
                break
            elif '\"responseStatus\":\"OK\"' not in return_page:
                print("""
%s: Metadata job for client: %s did not return correct reponse""" % (
                    today(), job))
                if attempt >= 2:
                    attempt = 0
                    time.sleep(2)
                    break
            else:
                print("""
%s: Metadata job for client: %s failed to trigger for some reason""" % (
                    today(), job))
                if attempt >= 2:
                    attempt = 0
                    time.sleep(2)
                    break
            time.sleep(2)
            attempt += 1


if __name__ == "__main__":
    # main(sys.argv[1:])
    main()
