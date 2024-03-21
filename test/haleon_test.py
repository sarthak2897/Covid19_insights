# # Given an input Array/List, determine if the array/list is sorted or not.
# # The sorting could be either in ascending or descending order.
# # Equal elements are considered as sorted. The output should be True or False.
# # No inbuilt methods should be used.
#
# #Input = [1,3,3,5,7]
# #Output = True
#
# #Input = [18,15,11,10,9,1]
# #Output = True
#
# #Input = [1,1,1]
# #Output = True
#
# def is_sorted(input, flag):
#     isSorted = True
#     for i in range(0,len(input)):
#         for j in range(i+1,len(input)):
#             if flag == 'asc':
#                 if input[i] <= input[j]:
#                     continue
#             else:
#                 if input[i] >= input[j]:
#                     continue
#             else:
#                 isSorted = False
#                 break
#
#     return isSorted
#
# print(is_sorted([18,15,11,10,9,1]))
# #print(is_sorted([1,3,3,5,7]))
# #print(is_sorted([1,1,1]))



#id  value
#1   {"a": "Test", "b": "XYZ", "c": [1,2,3,4]}

#id  a     b      c
#1  Test  XYZ     1
#1  Test  XYZ     2
#1  Test  XYZ     3
#1  Test  XYZ     4

#df = [for val in df.value.c]