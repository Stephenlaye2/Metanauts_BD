import re
data = ''
clean_data = []
word_count = {}

# Open and read the file
with open('shakespeare.txt', 'r') as shakespeare_file:
  file_data = shakespeare_file.read()
  data = file_data


#Use regular expression to match the words
p = re.compile('\w+[-_.\'a-zA-Z]*')

# Find all the matched word and store them in a list
matched_data = p.findall(data)

print(matched_data)

for word in matched_data:
  if not (word.isnumeric()):
    clean_data.append(word)
   
  
print(len(matched_data))
word_count['word_count'] = len(clean_data)
print(word_count)


