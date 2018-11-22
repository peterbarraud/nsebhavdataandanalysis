ECHO off

IF EXIST mdb.running (
	ECHO The DB seems to be running. Please check
	pause
) ELSE (
	ECHO MariaDB running > mdb.running
	bin\mysqld || (
		pause
	)
)
