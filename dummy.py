import ravop as R

R.initialize('dummy_test')
algo = R.Graph(name='dummy_test', algorithm='test', approach='distributed')

a = R.t([[1,2,4,5,6],[2,3,4,5,7]])
d = a.ravel()
print("d: ", d())


algo.end()