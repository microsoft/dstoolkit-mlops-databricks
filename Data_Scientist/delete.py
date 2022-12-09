arrayTest = [[1,2,3],[4,5,6],[7,8,9]] 

for element in arrayTest:
    print(tuple(element[2]))

#[tuple(element[2][0][0])[0] for element in arrayTest]
results = []



lhs = []
rhs = []
for result in results:
    lhsTuple = tuple(result[2][0][0])[0]
    lhs.append(lhsTuple)

    rhsTuple = tuple(result[2][0][1])[0]
    rhs.append(lhsTuple)


