#!/usr/bin/env python

################################################################################
#$Id: vtv_py.py,v 1.1 2016/03/23 12:50:40 headrun Exp $
#Copyright(c) 2005 Veveo.tv
################################################################################

import hashlib
import hmac

from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from platform import linux_distribution


def get_md5_hash():
    md = hashlib.md5()
    return md

def get_new_hmac(password, challenge):
    obj = hmac.new(password, challenge, hashlib.sha1)
    return obj


