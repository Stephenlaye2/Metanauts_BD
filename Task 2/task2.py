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
p = re.compile('[a-zA-Z][-_\'a-zA-Z]*')

# Find all the matched word and store them in a list
matched_data = p.findall(data.lower())
# print(matched_data)

for word in matched_data:
  # Separate words with two dashes in between them
  if '--' in word:
    two_dashes_words += f'{word}--'
    continue
    # Append other words to clean_data
  clean_data.append(word)

# Split words with two dashes by --
new_string = two_dashes_words.replace('----', '--')
split_dashes = new_string.split('--')

# Remove the empty string from the array
split_dashes.remove('')

# Add both the array of clean_data and split_dashes
combined_words = clean_data + split_dashes

"""
FOR DEBUGING PURPOSE
print(combined_words)
print(len(matched_data))
print(two_dashes_words.replace('----', '--'))
print(split_dashes)
print(len(split_dashes)-1)
"""

# Count each word in the array
word_count['Total Words'] = len(combined_words)
joined_word = ' '.join(combined_words)
for word in combined_words:
  count = joined_word.count(word)
  word_count[word] = count

# print(joined_word)
print(word_count)

