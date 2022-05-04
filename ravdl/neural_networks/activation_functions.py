import ravop as R

class Sigmoid():
    def __call__(self, x):
        return R.div(R.t(1), R.add(R.t(1), R.exp(R.neg(x))))

    def gradient(self, x):
        return self.__call__(x) * (R.t(1) - self.__call__(x))

class Softmax():
    def __call__(self, x):
        e_x = R.exp(x - R.max(x, axis=-1, keepdims="True"))
        return R.div(e_x, R.sum(e_x, axis=-1, keepdims="True"))

    def gradient(self, x):
        return self.__call__(x) * (R.t(1) - self.__call__(x))

class TanH():
    def __call__(self, x):
        return R.div(R.t(2), R.t(1) + R.exp(R.neg(R.t(2)) * x)) - R.t(1)

    def gradient(self, x):
        return R.t(1) - R.pow(self.__call__(x), R.t(2))

class ReLU():
    def __call__(self, x):
        condition = R.greater_equal(x, R.t(0))
        return R.where(x,R.t(0),condition=condition().tolist())

    def gradient(self, x):
        condition = R.greater_equal(x, R.t(0))
        return R.where(R.t(1),R.t(0),condition=condition().tolist())