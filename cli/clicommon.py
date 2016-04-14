import sys
import requests

class CLI(object):
    def __init__(self,
                 overlord='OVERLORD',
                 usage="Usage: ...",
                 args_expected=0):
        self.usage = usage
        self.expected = args_expected + 1
        self.overlord = overlord

        # Check environment vars
        if self.overlord is None:
            print("Overlord url is not set!".format(overlord))
            exit(1)

        # Check arguments
        if len(sys.argv) != self.expected:
            print("Invalid arguments")
            print(self.usage)
            exit(2)

    def GET(self, request):
        url = '{0}{1}'.format(
            self.overlord,
            request
        )
        res = requests.get(url)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print("HTTP Error: {0}".format(e.message))
            print("Response: {0}".format(res))
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
            print("HTTP Error: {0}".format(e.message))
            print("Response: {0}".format(res.json()))
            exit(3)

        return res.json()
