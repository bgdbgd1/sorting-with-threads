# Find the highest number that is divisible with 3 having as input a list of digits. A digit can appear multiple times in the input list but each digit from the input list can be used only once. If no number is found, return 0.
# Example: [1,4,4] => 441; [4,1,4] => 441; [1,2,3,4] => 321 [4,4] => 0



from itertools import combinations
import numpy


def solution_3(arr):
    # using set to deal
    # with duplicates
    n = len(arr)
    while n > 0:
        resulted_list = list(combinations(arr, n))
        for combo in resulted_list:
            if sum(combo) % 3 == 0:
                combo = sorted(combo)
                final_value = 0
                for n_factor, digit in enumerate(combo):
                    final_value += digit * 10 ** n_factor
                return final_value
        n -= 1
    return 0


def combs(lst, n):
    if n == 0:
        return [[]]

    l = []
    for i in range(0, len(lst)):

        m = lst[i]
        remLst = lst[i + 1:]

        for p in combs(remLst, n - 1):
            l.append([m] + p)

    return l


def solution_2(lst):
    lst = sorted(lst, reverse=True)
    n = len(lst)
    while n > 0:
        resulted_list = combs(lst, n)
        for combo in resulted_list:
            if sum(combo) % 3 == 0:
                combo = sorted(combo)
                final_value = 0
                for n_factor, digit in enumerate(combo):
                    final_value += digit * 10 ** n_factor
                return final_value
        n -= 1
    return 0


# Driver Function
if __name__ == "__main__":
    print(solution_2([3, 1, 4, 1, 5, 9]))
