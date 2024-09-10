import pandas as pd
import warnings
 
warnings.filterwarnings('ignore')
pd.set_option("expand_frame_repr", False)
 
df= pd.read_csv(r"C:\Projects\IndexToolKit(All)\EconsightMetaverse\Eco.csv")
#print(df)
 
df_new = pd.DataFrame()
df_new['stoxxId'] = ''
 
vendorNames = ['EconsightMetaverse', 'EconsightLithiumBatteries' ,'EconsightEnergyPatent']
columns = ['technologyName','technologyCode','totalPatentsInTechnology','worldClassPatentsInTechnology','specialisation']
 
for vendorName in vendorNames :
    #print(vendorName)
    df_temp = df[df['name']==vendorName]
    df_temp = df_temp[['stoxxId','technologyName','technologyCode','totalPatentsInTechnology','worldClassPatentsInTechnology','specialisation']]
    #print(df_temp.head())
    df1 =  df_temp.groupby('stoxxId').apply( lambda x: x[columns].to_dict(orient='records')).reset_index(name='technologydetails')
    #df1=pd.DataFrame(dicts, index=dicts.index, columns=['technologydetails'])
 
    #print(df1.head())
 
    df0 =  df[df['name']==vendorName]
    df0 = df0[['stoxxId','owner','totalPatents','environment','environmentWorldClass']]
    df0= df0.drop_duplicates()
    #print(df0.head())
    #df0.to_csv(r"C:\Users\sradhakrishnan\Downloads\econsight_interim.csv",index=False)
 
 
    df_all = pd.merge(df0,df1,on='stoxxId',how='left')
 
   
    #print(df_all.head())
 
    df_all.columns = [f'{i}@{vendorName}' if i not in ['stoxxId'] else f'{i}' for i in df_all.columns]
    df_new = pd.merge(df_new,df_all,on='stoxxId',how='outer')
 
print(df_new.head(3))
df_new.to_csv('check.csv',index=False)