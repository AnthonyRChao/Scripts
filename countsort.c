/**
 * Sorts array of n values O(n+k).
 */

#include <stdio.h>

void sort(int values[], int n)
{
    // count sort implementation
    const int MAX = 65536;

    // print original array
    for (int j = 0; j < n; j++)
    {
        printf("%i ", values[j]);
    }
    printf("\n\tprinted values[]!\n");

    // create countArr
    int countArr[MAX];
    for (int i = 0; i < MAX; i++)
    {
        countArr[i] = 0;
        printf("countArr[%i] = %i\n", i, countArr[i]);
    }
    printf("\tcreated countArr!\n");

    // iterate over original array, increment countArr
    for (int k = 0; k < n; k++)
    {
        countArr[values[k]]++;
        printf("incremented countArr[%i]\n", values[k]);
    }

    // print updated countArr
    for (int l = 0; l < MAX; l++)
    {
        printf("countArr[%i] = %i\n", l, countArr[l]);
    }
    printf("\tupdated countArr!\n");

    // accumatively add each pair of consecutive values in countArr
    printf("countArr[%i] = %i\n", 0, countArr[0]);
    for (int m = 1; m < MAX; m++)
    {
        if (m < MAX )
        {
            countArr[m] = countArr[m] + countArr[m-1];
            printf("countArr[%i] = %i\n", m, countArr[m]);
        }
    }
    printf("\tadded consecutive values in countArr!\n");

    // shift countArr over by one index
    for (int p = MAX - 1; p > 0; p--)
    {
        countArr[p] = countArr[p-1];
    }
    countArr[0] = 0;

    // print shifted countArr
    for (int q = 0; q < MAX; q++)
    {
        printf("countArr[%i] = %i\n", q, countArr[q]);
    }
    printf("\tshifted countArr!\n");

    // create newArr to hold sorted elements
    int newArr[n];

    // iterate through original array and sort elements with values, countArr & newArr
    for (int r = 0; r < n; r++)
    {
        // find index of countArr that corresponds with element in values
        newArr[countArr[values[r]]] = values[r];
        countArr[values[r]]++;
    }

    // print newArr
    for (int s = 0; s < n; s++)
    {
        printf("newArr[%i] = %i\n", s, newArr[s]);
    }
    printf("\tprinted newArr!\n");

    // copy integer array to another array
    for (int t = 0; t < n; t++)
    {
        values[t] = newArr[t];
    }

    // print values[]
    for (int u = 0; u < n; u++)
    {
        printf("values[%i] = %i\n", u, values[u]);
    }
    printf("\tsorted values[]!\n");

}