from pwn import *
import sys

context.log_level = 'error'

attempts = int(sys.argv[1])
print(attempts)


for i in range(attempts):
    # try:
    #     os.remove(FILE_NAME)
    # except:
    #     pass
    print("Test ",(i+1))
    p = process(["go","test","-run","3D"])

    result = p.readallS()
    print(result)
    if ("FAIL" in result) or ("Error" in result):
        break


# TestUnreliableChurn3C

# TestReliableChurn3C
