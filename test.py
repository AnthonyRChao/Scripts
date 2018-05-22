# x = 1
# y = x
# x = x + 1
# print(x)
# print(y)

# x = [1, 2, 3]
# y = x
# x[0] = 4
# print(x)
# print(y)

x = ['foo', [1, 2, 3], 10.4]
y = list(x)  # or x[:]
y[0] = 'foooooo'
y[1][0] = 4
print(x)
print(y)
