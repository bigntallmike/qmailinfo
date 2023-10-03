#!/usr/bin/env python3
#
# Copyright (C) 2023 Michael T. Babcock
# 
# This program is licensed under the terms of the LGPL license.
#
# A Library for parsing qmail log files from journald, as in:
# $ journalctl -u qmail --since=today | associate.py
#
# Additional functionality to come

from datetime import datetime
import re
import sys

class qmailLog:
    def __init__(self):
        self._messages = {} # list of messages by qmail id
        self._deliveries = {} # list of deliveries by qmail id
        self._senders = {} # list of messages associated with this sender
        self._recipients = {} # list of messages associated with this recipient
        self._re_logline = re.compile(
            r'(?P<month>\S+)\s+'
            r'(?P<day>[0-9]{1,2})\s+'
            r'(?P<time>[0-9]+:[0-9]+:[0-9]+)\s+'
            r'(?P<hostname>\S+)\s+'
            r'(?P<service>\S+):\s+'
            r'(?P<qmaillog>.*)'
            )
        self._re_new = re.compile(r'new msg (?P<msgid>\d+)')
        self._re_end = re.compile(r'end msg (?P<msgid>\d+)')
        self._re_delivery = re.compile(
            r'starting delivery (?P<deliveryid>\d+): '
            r'msg (?P<msgid>\d+) to (?P<localornot>\S+) (?P<recipient>\S+)'
            )
        self._re_info = re.compile(
            r'info msg (?P<msgid>\d+):\s+'
            r'bytes (?P<bytes>\d+)\s+'
            r'from <(?P<emailaddr>\S+@\S+)>\s+'
            r'qp (?P<qp>\d+)\s+'
            r'uid (?P<uid>\d+)'
            )

    def parse(self, line):
        line = line.strip()
        if data := self._re_logline.match(line):
            data = data.groupdict()
        else:
            return

        timestamp = datetime.strptime("%(month)s %(day)s %(time)s" % data, '%b %d %H:%M:%S')

        qmaillog = data['qmaillog']

        if match := self._re_new.match(qmaillog):
            self.new_message(timestamp, int(match.groupdict()['msgid']))
        elif match := self._re_end.match(qmaillog):
            msgid = int(match.groupdict()['msgid'])
            self.new_message(timestamp, msgid)
            self._messages[msgid]['done'] = {
                'end': timestamp
            }
            print("END %d: %s" % (msgid, self._messages[msgid]))
        elif match := self._re_delivery.match(qmaillog):
            delivery = match.groupdict()
            msgid = int(delivery['msgid'])
            self.new_message(timestamp, msgid)
            self._deliveries[int(delivery['deliveryid'])] = delivery
            self._messages[msgid]['deliveries'].append(delivery)
        elif match := self._re_info.match(qmaillog):
            msgid = int(match.groupdict()['msgid'])
            self.new_message(timestamp, msgid)
            self._messages[msgid]['sender'] = match.groupdict()['emailaddr']
        else:
            print("Unknown")

    def new_message(self, timestamp, msgid):
        if not msgid in self._messages:
            print("Unknown message id %d" % msgid)
            self._messages[msgid] = {
                'start': timestamp, 
                'deliveries': []
                }


if __name__ == '__main__':
    logdata = qmailLog()
    for line in sys.stdin:
        logdata.parse(line)