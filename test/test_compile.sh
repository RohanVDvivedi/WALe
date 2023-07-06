gcc ./test_swrite.c ./test_util.c -o swrite.out -lblockio -lwale -lrwlock -lpthread -lcutlery

gcc ./test_read.c ./test_util.c -o read.out -lblockio -lwale -lrwlock -lpthread -lcutlery

gcc ./test_prwrite.c ./test_util.c -o prwrite.out -I./ -lboompar -lblockio -lwale -lrwlock -lpthread -lcutlery

gcc ./test_prwrite_validate.c ./test_util.c -o prwrite_validate.out -I./ -lblockio -lwale -lrwlock -lpthread -lcutlery