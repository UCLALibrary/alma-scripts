#!/usr/bin/env python3
from sftp_credentials import PAC
import pysftp


def get_pac_error_file():
    remote_file = "BATCH-AP-LIBRY-ERR"
    local_file = "/tmp/" + remote_file
    with pysftp.Connection(
        PAC["server"], username=PAC["user"], password=PAC["password"]
    ) as sftp:
        print("Connected")
        sftp.get(remote_file, local_file)
        # Get full directory listing
        for line in sftp.listdir_attr():
            print(line)


def main() -> None:
    get_pac_error_file()


if __name__ == "__main__":
    main()
