from itertools import combinations


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
                    final_value += digit * 10**n_factor
                return final_value
        n -= 1
    return 0


def combs(lst, n):

    if n == 0:
        return [[]]

    while sum(lst) % 3 != 0:
        lst = lst[1:]

    l = []
    for i in range(0, len(lst)):

        m = lst[i]
        remLst = lst[i + 1:]

        for p in combs(remLst, n - 1):
            l.append([m] + p)

    return l


def solution_2(lst):
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
    print(solution_2([3,4,4,1]))
