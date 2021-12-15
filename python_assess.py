##input = [Monday, Monday, Tuesday, Monday, Thursday, Saturday, Friday, Wednesday, Monday, Wednesday, Thursday, Saturday, Sunday, Tuesday]

##output = {Monday : 4, Tuesday : 2, Wednesday : 2, Thursday : 2, Friday : 1, Saturday : 2, Sunday : 1}##input = [Monday, Monday, Tuesday, Monday, Thursday, Saturday, Friday, Wednesday, Monday, Wednesday, Thursday, Saturday, Sunday, Tuesday]

##output = {Monday : 4, Tuesday : 2, Wednesday : 2, Thursday : 2, Friday : 1, Saturday : 2, Sunday : 1}


arr = ["Monday", "Monday", "Tuesday", "Monday", "Thursday", "Saturday", "Friday", "Wednesday", "Monday", "Wednesday", "Thursday", "Saturday", "Sunday", "Tuesday"]
output = {}
count_mo = 0
count_tu = 0
count_we = 0
count_th = 0
count_fr = 0
count_sa = 0
count_su = 0

for lst in arr:
    if lst == "Monday":
         count_mo += 1;

    elif lst == "Tuesday":
        count_tu += 1
    elif lst == "Wednesday":
        count_we += 1
    elif lst == "Thursday":
        count_th += 1
    elif lst == "Friday":
        count_fr += 1
    elif lst == "Saturday":
        count_sa += 1
    elif lst == "Sunday":
        count_su += 1

output["Monday"] = count_mo
output["Tuesday"] = count_tu
output["Wednesday"] = count_we
output["Thursday"] = count_th
output["Friday"] = count_fr
output["Saturday"] = count_sa
output["Sundat"] = count_su

print(output)
