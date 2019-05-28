#!/bin/env python
import os
import time

print "[[[DUMMY]]]", "[[[ALT]]]"
print "[[[EXTRA]]]"

for i in range(4):
    os.system("dd if=/dev/zero of="+str(i)+".dat bs=10M count=1")
    os.system("cp "+str(i)+".dat [[[#OUTPUTPATH]]]")
    time.sleep(5)
