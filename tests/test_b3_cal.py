
import bizdays

cal = bizdays.Calendar.load('B3.cal')

print(cal)

months = list(range(1, 13))
years = list(range(2000, 2023))

# for year in years:
#     d1 = cal.getdate('first day', year)
#     d2 = cal.getdate('last day', year)
#     du = cal.bizdays(d1, d2) + 1
#     print(year, du)
    # for month in months:
    #     d1 = cal.getdate('first day', year, month)
    #     d2 = cal.getdate('last day', year, month)
    #     du = cal.bizdays(d1, d2) + 1
    #     print(year, month, du)

year = 2017
for month in months:
    d1 = cal.getdate('first day', year, month)
    d2 = cal.getdate('last day', year, month)
    du = cal.bizdays(d1, d2) + 1
    print(year, month, du)