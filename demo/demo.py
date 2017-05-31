import curses
import time

def demo_str(i):
    demostr = '   '
    for j in range(nnode):
        demostr += 'Node ' + str(j) + ': '
        ncommit = log[j][i][0]
        nlog = log[j][i][1]
        demostr += '|' * ncommit
        demostr += '_' * (nlog - ncommit)
        demostr += '\n   '
    return demostr


with open('./log') as f:
    lines = f.readlines()

nnode = 5
niter = len(lines) // 6
log = [[None] * niter for _ in range(nnode)]
for i in range(niter):
    for j in range(nnode):
        line = lines[i * (nnode + 1) + j]
        log[j][i] = [int(s) for s in line.split()]


screen = curses.initscr()
screen.keypad(1)
screen.nodelay(1)
curses.noecho()


print('Loading log')
time.sleep(2)


cnt = 0
while True and cnt < niter:
    screen.clear()
    screen.addstr('\n')
    screen.addstr('                  Raft Log Visualization        \n')
    screen.addstr('\n')
    screen.addstr(demo_str(cnt))
    cnt += 1
    key = screen.getch()
    if key == ord('q'):
        break
    time.sleep(0.05)

time.sleep(10)
curses.endwin()
