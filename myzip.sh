while : ; do ls -tr | grep csv$ | grep -v `date -d yesterday +%y%m%d`.csv | grep -v `date +%y%m%d`.csv | head -n 10 | xargs -I {} gzip {} ; echo -n . ; sleep 200 ; done
