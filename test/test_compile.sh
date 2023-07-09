gcc ./test_swrite.c ./test_util.c -o swrite.out -lblockio -lwale -lrwlock -lpthread -lcutlery -lz

gcc ./test_read.c ./test_util.c -o read.out -lblockio -lwale -lrwlock -lpthread -lcutlery -lz

gcc ./test_prwrite.c ./test_util.c -o prwrite.out -I./ -lboompar -lblockio -lwale -lrwlock -lpthread -lcutlery -lz

gcc ./test_prwrite_validate.c ./test_util.c -o prwrite_validate.out -I./ -lblockio -lwale -lrwlock -lpthread -lcutlery -lz

# use below command to change a byte anywhere in the file and see, how crc32 identifies this error
# printf '\x31' | dd of=test_blob bs=1 seek=100 count=1 conv=notrunc

# to pretty print the .log file you can use
# hexdump -e'"%07.8_ad  " 8/1 "%02x " "  " 8/1 "%02x " "  |"' -e'16/1  "%_p"  "|\n"' test.log
