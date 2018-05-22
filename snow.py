from random import randrange
import time

'''
Snow animation

Snow is simply the `#` symbol here. Half of the snow moves at 1 char
per frame, while the other half moves at 0.5 chars per frame. The
program ends when all the snow reaches the bottom of the screen.
The viewing area is 80x25. It puts 100 snow flakes to output, half
fast, and half slow. Every frame it dispenses 4 flakes, 2 fast and
2 slow ones, at random locations at the top of the viewing area.
'''

screen = {'x': 80, 'y': 20}
drops = []


def createRainDrop(x, y, speed):
    return {'x': x, 'y': y, 'speed': speed}


def createRandomDrops():
    dropCount = 4
    for i in range(dropCount):
        yield createRainDrop(randrange(0, screen['x']), 0, min((i % 2) + 0.5, 1))


def moveDrops():
    for drop in drops:
        speed = drop['speed']
        drop['y'] = drop['y'] + speed


def drawDrops():
    out = [''] * screen['y']
    for y in range(screen['y']):
        for x in range(screen['x']):
            out[y] += '#' if any([drop['x'] == x and int(drop['y']) == y for drop in drops]) else ' '

    return '\n'.join(out)


def dropsOnScreen():
    return any([drop['y'] < screen['y'] for drop in drops])


drops += createRandomDrops()

while dropsOnScreen():
    if len(drops) < 100:
        drops += createRandomDrops()

    print(drawDrops())
    moveDrops()
    time.sleep(0.100)
