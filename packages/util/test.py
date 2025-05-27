# just a comment
def make_populated_column(detn,variable):
  detn[f'{variable}_populated'] = detn[variable].notnull().astype(int)
  return detn,f'{variable}_populated'
