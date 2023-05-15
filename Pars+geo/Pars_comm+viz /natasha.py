#%%
import pandas as pd
data1 = pd.read_csv(r"C:\Users\vitaly.flerin\Desktop\result.csv" \
    , sep=',' , encoding='cp1251') #, index_col=0 - индекс 
data1.head()  
#%%
df = data1
df['mark_6']=df['mark_6'].apply(str)
df
# %%
# !pip install natasha
import pandas as pd
from natasha import AddrExtractor, MorphVocab

morph_vocab = MorphVocab()
extractor = AddrExtractor(morph_vocab)

def fix_addr(text):
    matches = extractor(text)
    return (', '.join(f'{match.fact.type or ""} {match.fact.value}' for match in matches)) or None

df['city']=df['mark_6'].map(fix_addr)
df

# %%
df.to_excel(r"C:\Users\vitaly.flerin\Desktop\res_natasha.xlsx", index=False)
# %%
