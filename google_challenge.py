def is_prime_number(num):
    # If given number is greater than 1
        # Iterate from 2 to n / 2
        for i in range(2, int(num / 2) + 1):

            # If num is divisible by any number between
            # 2 and n / 2, it is not prime
            if (num % i) == 0:
                return False
        return True


def solution(index):
    prime_string = '2'
    current_value = 3
    while len(prime_string) < index + 5:
        if is_prime_number(current_value):
            prime_string += str(current_value)
        current_value += 2
    return prime_string[index:index+5]


if __name__ == '__main__':
    sol = solution(3)
    print(sol)
