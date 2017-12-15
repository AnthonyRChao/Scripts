#define _XOPEN_SOURCE
#include <unistd.h>
#include <stdio.h>
#include <cs50.h>
#include <string.h>
#include <ctype.h>

int main(int argc, string argv[])
{
    if (argc != 2)
    {
        printf("Usage: ./crack k\n");
        return 1;
    }

    char key[5] = { '\0', '\0', '\0', '\0', '\0' };
    char salt[3] = { '5', '0', '\0' };
    string dict = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    int dict_length = strlen(dict) + 1; // plus 1 to include null terminator
    int maxTry = 9999999;

    // loop maxTry times
    for (int i = 1; i < maxTry; i++)
    {
        int index = 0;
        int k = i;

        while (k > 0 && k % dict_length != 0)
        {
            /*
            in each while loop iteration, set key[index] to a char in dict
            use dict_index to wraparound dict items
            */
            int dict_index = k % dict_length;
            key[index] = dict[dict_index - 1];
            /*
            since integers are whole numbers, the line below determines how many
            times the while loop will repeat based on the while loop criteria
            e.g.
            k =  1/3 will equal 0, do not repeat
            k =  4/3 will equal 1, repeat 1 time
            k = 11/3 will equal 3, repeat 3 times
            */
            k = k / dict_length;
            /*
            increment the index so that if k is > 0, the while loop will
            test chars for the next index position of key
            */
            index++;

            /*
            if (k == 0)
            {
                printf("i: %i, index: %i, dict_index = %i, key: %s\n", i, index, dict_index, key);
            }

            check if crypt output with current key matches hashed password input
            */
            if (strcmp(crypt(key, salt), argv[1]) == 0)
            {
                printf("%s\n", key);
                return 0;
            }
        }
    }

    printf("No match found.\n");
    return 1;
}

/*
username:hash
andi:50.jPgLzVirkc        | key: hi (seconds)
jason:50YHuxoCN9Jkc       | key: JH (seconds)
malan:50QvlJWn2qJGE       | key: NOPE (hr)
mzlatkova:50CPlMDLT06yY   | key: ha (seconds)
patrick:50WUNAFdX/yjA     | key: Yale (minute)
rbowden:50fkUxYHbnXGw     | key: rofl (minute)
summer:50C6B0oz0HWzo      | key: FTW (seconds)
stelios:50nq4RV/NVU0I     | key: ABC (seconds)
wmartin:50vtwu4ujL.Dk     | key: haha (seconds)
zamyla:50i2t3sOSAZtk      | key: lol (seconds)
*/
