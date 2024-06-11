#!/bin/sh
# Retrieve PAC invoice error file from campus SFTP server,
# show contents (if any), and email message to users.

# Get file from SFTP server using python, since native sftp
# client has no(?) support for password auth via command line.
/home/exlsupport/alma-scripts/get_alma_invoice_errors.py

echo "======================================================"

DATE=`date "+%Y%m%d"` #YYYYMMDD
FILE=/tmp/BATCH-AP-LIBRY-ERR
if [ -s ${FILE} ]; then
  echo "PAC INVOICE ERRORS ${DATE}:"
  cat ${FILE}
else
  echo "No PAC invoice errors ${DATE}"
fi
