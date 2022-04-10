# As Commander Lambda's personal assistant, you've been assigned the task of configuring the LAMBCHOP doomsday
# device's axial orientation gears. It should be pretty simple -- just add gears to create the appropriate rotation
# ratio. But the problem is, due to the layout of the LAMBCHOP and the complicated system of beams and pipes
# supporting it, the pegs that will support the gears are fixed in place. The LAMBCHOP's engineers have given you
# lists identifying the placement of groups of pegs along various support beams. You need to place a gear on each peg
# (otherwise the gears will collide with unoccupied pegs). The engineers have plenty of gears in all different sizes
# stocked up, so you can choose gears of any size, from a radius of 1 on up. Your goal is to build a system where the
# last gear rotates at TWICE THE RATE (in revolutions per minute, or rpm) of the first gear, no matter the direction.
# Each gear (except the last) touches and turns the gear on the next peg to the right.

# Given a list of distinct positive integers named pegs representing the location of each peg along the support beam,
# write a function solution(pegs) which, if there is a solution, returns a list of two positive integers a and b
# representing the numerator and denominator of the first gear's radius in its simplest form in order to achieve the
# goal above, such that radius = a/b. The ratio a/b should be greater than or equal to 1. Not all support
# configurations will necessarily be capable of creating the proper rotation ratio, so if the task is impossible,
# the function solution(pegs) should return the list [-1, -1]. For example, if the pegs are placed at [4, 30, 50],
# then the first gear could have a radius of 12, the second gear could have a radius of 14, and the last one a radius
# of 6. Thus, the last gear would rotate twice as fast as the first one. In this case, pegs would be [4, 30,
# 50] and solution(pegs) should return [12, 1]. The list pegs will be given sorted in ascending order and will
# contain at least 2 and no more than 20 distinct positive integers, all between 1 and 10000 inclusive.

def calculate_values(radiuses, pegs, previous_val, step):
    if step + 1 == len(pegs):
        val = pegs[step] - pegs[step-1] - previous_val
        radiuses.append(val)
        if radiuses[0] / 2 == radiuses[-1]:
            # radiuses.append(val)
            return radiuses
        else:
            return []

    if step < len(pegs):
        val = pegs[step] - pegs[step-1] - previous_val
        diff = pegs[step + 1] - pegs[step]
        if diff > val:
            radiuses.append(val)
            return calculate_values(radiuses, pegs, val, step+1)
        else:
            return []


def solution(pegs):
    diff1 = pegs[1] - pegs[0]
    diff_last = pegs[-1] - pegs[-2]
    if diff1 / 2 > diff_last:
        last_iter = diff_last
    else:
        last_iter = diff1

    for val1 in range(2, last_iter, 2):
        radiuses = [val1]
        radiuses = calculate_values(radiuses, pegs, val1, 1)
        if len(radiuses) == len(pegs):
            return [radiuses[0], 1]
    return [-1, -1]


if __name__ == "__main__":
    print(solution([4,15,32,100,170]))
    # print(solution([4, 30, 50])) # -> 12, 14, 6 -> 12, 1