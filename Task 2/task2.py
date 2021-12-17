import re
data = ''
clean_data = []
two_dashes_words = ''
word_count = {}

# Open and read the file
with open('shakespeare.txt', 'r') as shakespeare_file:
  file_data = shakespeare_file.read()
  data = file_data


#Use regular expression to match the words
p = re.compile('[a-zA-Z][-_.\'a-zA-Z]*')

# Find all the matched word and store them in a list
matched_data = p.findall(data)
print(matched_data)

for word in matched_data:
  if '.' in word and len(word) == 2:
    print(word)
    continue
  elif '--' in word:
    print(word)
    two_dashes_words += f'{word}--'
    continue

  clean_data.append(word)

new_string = two_dashes_words.replace('----', '--')
split_dashes = new_string.split('--')

"""
FOR DEBUGING PURPOSE
print(len(matched_data))
print(two_dashes_words.replace('----', '--'))
print(split_dashes)
print(len(split_dashes)-1)
"""

word_count['word_count'] = len(clean_data) + len(split_dashes)-1
print(word_count)

