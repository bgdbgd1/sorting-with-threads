from itertools import combinations


# def solution(l):
#     def calculate_value(lst):
#         final_value = 0
#         for n_factor, digit in enumerate(lst):
#             final_value += digit * 10 ** n_factor
#         return final_value
#
#     if len(l) == 0 or len(l) > 9:
#         raise Exception("list must contain between 1 and 9 digits")
#     else:
#         for i in l:
#             if i < 0 or i > 9:
#                 raise Exception("All elements of the list must be digits")
#
#     if sum(l) % 3 == 0:
#         l = sorted(l)
#         return calculate_value(l)
#
#     n = len(l) - 1
#     while n > 0:
#         resulted_list = list(combinations(l, n))
#         for combo in resulted_list:
#             if sum(combo) % 3 == 0:
#                 combo = sorted(combo)
#                 final_value = 0
#                 for n_factor, digit in enumerate(combo):
#                     final_value += digit * 10 ** n_factor
#                 return final_value
#         n -= 1
#     return 0


def solution(l):
    def combs(lst, n):
        if n == 0:
            return [[]]

        l = []
        for i in range(0, len(lst)):

            m = lst[i]
            remLst = lst[i + 1:]

            for p in combs(remLst, n - 1):
                new_comb = [m] + p
                if new_comb not in l:
                    l.append([m] + p)

        return l

    def calculate_value(lst):
        final_value = 0
        for n_factor, digit in enumerate(lst):
            final_value += digit * 10 ** n_factor
        return final_value

    if len(l) == 0 or len(l) > 9:
        raise Exception("list must contain between 1 and 9 characters")
    else:
        for i in l:
            if i < 0 or i > 9:
                raise Exception("All elements of the list must be digits")

    if sum(l) % 3 == 0:
        l = sorted(l)
        return calculate_value(l)

    n = len(l) - 1
    while n > 0:
        resulted_list = combs(l, n)
        for combo in resulted_list:
            if sum(combo) % 3 == 0:
                combo = sorted(combo)
                return calculate_value(combo)
        n -= 1
    return 0


# Driver Function
if __name__ == "__main__":
    print(solution([1,4,4,4,1,4]))
