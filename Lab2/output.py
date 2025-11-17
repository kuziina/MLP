from typing import Generic, TypeVar
T = TypeVar('T')
class Container(Generic[T]):
    def __init__(self, value: T):
        self.value = value

    def set_value(self, val):
        self.value = val

    def get_value(self):
        return self.value

    def print(self):
        print(f'Container: {self.value}')

    def increment(self):
        self.value += 1

    def decrement(self):
        self.value -= 1

    def is_positive(self):
        return self.value > 0

    def is_negative(self):
        return self.value < 0

    def is_zero(self):
        return self.value == 0

    def operate_multiple_times(self, times):
        for i in range(times):
                self.increment()

    def reset(self):
        self.value = 0

    def add(self, v):
        self.value += v

    def subtract(self, v):
        self.value -= v

    def multiply(self, v):
        self.value *= v

    def divide(self, v):
        if v != 0:
                self.value /= v

    def get_double(self):
        return self.value * 2

    def get_half(self):
        return self.value / 2

    def square(self):
        self.value = self.value * self.value

class PointerExample:
    def __init__(self, val):
        self.ptr = [val]

    def increment(self):
        self.ptr[0] += 1

    def decrement(self):
        self.ptr[0] -= 1

    def get_value(self):
        return self.ptr[0]

    def reset(self, val=0):
        self.ptr[0] = val

    def add(self, v):
        self.ptr[0] += v

    def subtract(self, v):
        self.ptr[0] -= v

    def print(self):
        print(f'Pointer value: {self.ptr[0]}')

class OperatorOverload:
    def __init__(self, v):
        self.value = v

    def __add__(self, other):
        return OperatorOverload(self.value + other.value)

    def __sub__(self, other):
        return OperatorOverload(self.value - other.value)

    def __mul__(self, other):
        return OperatorOverload(self.value * other.value)

    def __truediv__(self, other):
        if other.value == 0:
            raise ZeroDivisionError('Division by zero')
        return OperatorOverload(self.value / other.value)

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return self.value != other.value

    def print(self):
        print(f'Value: {self.value}')

    def get_value(self):
        return self.value

    def set_value(self, v):
        self.value = v

def main():
    cont = Container(10)
    cont.print()
    cont.operate_multiple_times(5)
    cont.print()
    cont.square()
    cont.print()
    p = PointerExample(100)
    p.print()
    p.increment()
    p.print()
    p.add(50)
    p.print()
    a = OperatorOverload(50)
    b = OperatorOverload(20)
    c = a + b
    c.print()
    d = a - b
    d.print()
    e = a * b
    e.print()

if __name__ == '__main__':
    main()
