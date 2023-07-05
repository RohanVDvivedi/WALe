gcc ./test_swrite.c ./test_util.c -o swrite.out -lwale -lcutlery -lblockio

gcc ./test_read.c ./test_util.c -o read.out -lwale -lcutlery -lblockio

gcc ./test_prwrite.c ./test_util.c -o prwrite.out -lwale -lboompar -lpthread -lcutlery -lblockio