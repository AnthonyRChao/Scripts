def spiral_copy(inputMatrix):

  numRows = len(inputMatrix)
  numCols = len(inputMatrix[0])
  
  topRow = 0
  btmRow = numRows - 1
  leftCol = 0
  rightCol = numCols - 1
  result = []
  
  while (topRow <= btmRow and leftCol <= rightCol):
    
    # Copy the uppermost row from left to right
    for i in range(leftCol, rightCol + 1):
      result.append(inputMatrix[topRow][i])
    
    topRow += 1
    
    # Copy the rightmost column from top to bottom
    for i in range(topRow, btmRow):
      result.append(inputMatrix[i][rightCol])

    rightCol -= 1
  
    # Copy the lowermost row from right to left
    if (topRow <= btmRow):
      for i in range(rightCol + 1, leftCol - 1, -1):
        result.append(inputMatrix[btmRow][i])
    
      btmRow -=1
      
    # Copy the leftmost column from bottom to top
    if (leftCol <= rightCol):
      for i in range(btmRow, topRow - 1, -1):
        result.append(inputMatrix[i][leftCol])
    
      leftCol += 1
    
  return result

"""
input:  inputMatrix  = [ [1,    2,   3,  4,    5],
                         [6,    7,   8,  9,   10],
                         [11,  12,  13,  14,  15],
                         [16,  17,  18,  19,  20] ]

output: [1, 2, 3, 4, 5, 10, 15, 20, 19, 18, 17, 16, 11, 6, 7, 8, 9, 14, 13, 12]
"""
  
  
  
  
