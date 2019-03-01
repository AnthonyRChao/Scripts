import logging


logging.basicConfig(level=logging.DEBUG)


class Pizza:
    def __init__(self, init_name, init_price):
        self.name = init_name
        self.price = init_price
        logging.debug('Pizza created: {} (${})'.format(self.name, self.price))

    def make(self, quantity=1):
        logging.debug("Made {} {} pizza(s)".format(quantity, self.name))

    def eat(self, quantity=1):
        logging.debug("Ate {} pizza(s)".format(quantity))


pizza_01 = Pizza('artichoke', 15)
pizza_01.make()
pizza_01.eat()

pizza_02 = Pizza('margherita', 12)
pizza_02.make()
pizza_02.eat()

print(logging.DEBUG)
print(logging.INFO)
print(logging.WARNING)
print(logging.ERROR)
print(logging.CRITICAL)


logger1 = logging.getLogger('module_1')
logger2 = logging.getLogger('module_2')

logger1.debug('Module 1 debugger')
logger2.debug('Module 2 debugger')

