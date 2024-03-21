input = 'Sarthak Nagpal Sarthak'

input1 = 'My name is Sarthak Nagpal and I am a Data Engineer working in Hashedin By Deloitte'

print(input1.title())
words = [word.capitalize() for word in input.split(" ")]
print(type(words))
print(" ".join(words))

print(sorted(set(words)))

print(max(words))

def count_substring_occurrences(words,substring):
    index = 0
    count = 0
    while True:
        index = words.find(substring,index)
        if index == -1:
            break
        index = index + 1
        count = count + 1
    return count

def most_frequent_word(input):
    char_count = {}
    for word in input.split(' '):
        char_count[word] = char_count.get(word,0) + 1
    return max(char_count)

def count_word_occurrences(input):
    word_count = 0
    for word in input.split(' '):
        word_count = word_count + 1
    return word_count

def common_strings(input,input1):
    common_words = [char for char in set(input) if char in input1]
    return " ".join(common_words)

def second_most_frequent_word(input):
    char_count = {}
    for word in input.split(' '):
        char_count[word] = char_count.get(word,0) + 1
    sorted_char_count = sorted(char_count.items(),key=lambda x:x[1],reverse=True)
    return sorted_char_count[1][0] if len(sorted_char_count) >= 2 else None




print(count_substring_occurrences("".join(words),'Sarthak'))

print(most_frequent_word(input1))
print(count_word_occurrences(input1))
print(" ".join(reversed(input1.split(' '))))
print(common_strings(input.split(' '), input1.split(' ')))
print(second_most_frequent_word(input))

a=[1,2,3,4,5,6,7,8,9]
a[::2]=10,20,30,40,50
print(a)

a = {(1,2):1,(2,3):2}
print(a[(1,2)])

my_dict = {1: 1, '1': 2, 1.0: 4}

print(my_dict)