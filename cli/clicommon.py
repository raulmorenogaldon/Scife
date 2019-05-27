import sys
import requests

class CLI(object):
    def __init__(self,
                 overlord='OVERLORD',
                 token='SCIFE_TOKEN',
                 usage="Usage: ...",
                 args_expected=0):
        self.usage = usage
        self.expected = args_expected + 1
        self.overlord = overlord
        self.token = token

        # Check environment vars
        if self.overlord is None:
            sys.stderr.write("Overlord url is not set!\n".format(overlord))
            exit(1)

        # Check arguments
        if len(sys.argv) != self.expected:
            sys.stderr.write("Invalid arguments\n")
            sys.stderr.write(self.usage)
            sys.stderr.write("\n")
            exit(2)

    def GET(self, request):
        url = '{0}{1}'.format(
            self.overlord,
            request
        )
        res = requests.get(url, headers={'x-access-token':self.token})
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            sys.stderr.write("HTTP Error: {0}\n".format(e.message))
            sys.stderr.write("Response: {0}\n".format(res))
            exit(3)

        return res.json()

    def POST(self, request, data={}):
        url = '{0}{1}'.format(
            self.overlord,
            request
        )
        res = requests.post(url, data=data)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            sys.stderr.write("HTTP Error: {0}\n".format(e.message))
            sys.stderr.write("Response: {0}\n".format(res.json()))
            exit(3)

        return res.json()
