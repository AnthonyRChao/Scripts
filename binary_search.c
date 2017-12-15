// binary search implementation, O(log n)

#include <stdbool.h>
#include <stdio.h>

bool search(int value, int values[], int n);
bool binary_search(int value, int values[], int n, int low, int high);

int main(void)
{
    int size = 9;
    int haystack[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    int needle = 8;

    if (search(needle, haystack, size))
    {
        printf("\nFound needle in haystack!\n\n");
        return 0;
    }
    else
    {
        printf("\nDidn't find needle in haystack.\n\n");
        return 1;
    }
    return 0;
}

// required to maintain declaration of search function
bool search(int value, int values[], int n)
{
    // initialize low & high to feed to binary_search function
    int low = 0;
    int high = n - 1;

    if (binary_search(value, values, n, low, high))
    {
        return true;
    }
    else
    {
        return false;
    }
}

// binary search implementation
bool binary_search(int value, int values[], int n, int low, int high)
{
    int mid = (low + high) / 2;

    // base case
    if (values[mid] == value)
    {
        // printf("Value found at index %i.\n", mid);
        return true;
    }
    // recursive case 1
    else if (values[mid] > value)
    {
        // redefine high as left of prior mid
        return binary_search(value, values, n, low, mid - 1);
    }
    // recursive case 2
    else
    {
        // redfine low as one right of prior mid
        return binary_search(value, values, n, mid + 1, high);
    }
}



