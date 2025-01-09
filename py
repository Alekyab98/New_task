def get_pair(dev):
    map=pd.read_csv('map.csv')

    sap=dev[:8].upper()
    #handle specific cases as necessary
    if(sap=='BBTPNJ06'): #legacy branchburg CLLI, replace with current
        sap='BBTPNJDA' 
    elif(sap=='WNDSCTGJ'): #legacy windsor CLLI
        sap='WNDSCTWL' 
    elif(sap=='HSTXTXOM'): #legacy copperfield CLLI
        sap='HSNOTX08'
    elif(sap=='RCHLTXEK' or sap=='WELKTXLB'): #maintence engineer lab elemets - dropped ultimately from dataset
        pair='lab-element'
        region='lab-element'
        timezone='lab-element'

    #this was a troubleshoot mechanism to spot device names out of the ordinary
    #no devices should hit the except after the case handling above/below
    try:
        pair=map[map['Site CLLI']==sap]['Pair'].iloc[0]
        region=map[map['Site CLLI']==sap]['Region'].iloc[0]
    except:
        print(sap,pair)

    #for pgw/vpgw
    name={'PNC':'PND','PND':'PNC',
        'PNR':'PNS','PNS':'PNR',
        'PNE':'PNF','PNF':'PNE',
        'PNT':'PNU','PNU':'PNT',
        'PNH':'PNI','PNI':'PNH',
        'PNJ':'PNK','PNK':'PNJ',
        'VNE':'VNF','VNF':'VNE',
        'VNC-00':'VND-01','VND-01':'VNC-00',
        'VNE-00':'VNF-01','VNF-01':'VNE-00',
        'VNJ-00':'VNK-01','VNK-01':'VNJ-00',
        'VNX-00':'VNY-01','VNY-01':'VNX-00'}
    prims=['PNC','PNR','PNE','PNT','PNH','PNJ','VNE',
           'VNC-00','VNE-00','VNJ-00','VNX-00']

    #aether
    if('UPF' in dev):
        pairdev=pair+dev[8:-4] #instead of hard coding copy string slice from og device
        if(dev[25]=='P'):
            primary=True
            pairdev=pairdev+'S'
        else:
            primary=False
            pairdev=pairdev+'P'
        pairdev=pairdev+dev[-3:]
    elif('smf' in dev):
        pairdev=pair.lower()+dev[8:-5] #instead of hard coding copy string slice from og device
        if(dev[25]=='p'):
            primary=True
            pairdev=pairdev+'s'
        else:
            primary=False
            pairdev=pairdev+'p'
        pairdev=pairdev+dev[-4:]
        print(pairdev)
    #sevone
    elif('VPGW' in dev.upper()):
        dev=dev.upper()
        suffix=dev.split('-')[0][8:]
        pairdev=(pair+suffix+'-L-CI-'+name[dev[-6:]]).upper()
        if(dev[-6:] in prims):
            primary=True
        else:
            primary=False
    elif('SGW' in dev):
        #nokia
        if('AL-SGW' in dev):
            pairdev=pair+'91A-L-AL-SGW'
        #e//
        else:
            pairdev=pair+'91A-L-EC-SGW'
        if(dev[20]=='P'):
            pairdev=pairdev+'S'
            primary=True
        else:
            pairdev=pairdev+'P'
            primary=False
        pairdev=pairdev+dev[-3:]
    else: #PGW - not indicated otherwise
        pairdev=pair+name[dev[-3:]]
        if(pair=='HSNOTX08'):
            pairdev='HSTXTXOM'+name[dev[-3:]]
        if(dev=='HLBOOR38PNU'):
            pairdev='MILNHI04PNT'
        if(dev[-3:] in prims):
            primary=True
        else:
            primary=False
    try:
        if(primary):
            primsap=sap
            timezone=map[map['Site CLLI']==sap]['Timezone'].iloc[0] #always use timezone of primary (some pairs cross timezone)
        else:
            primsap=pair
            timezone=map[map['Site CLLI']==pair]['Timezone'].iloc[0]
    except:
        pass
    #print(primary,pairdev,primsap,region,timezone)
    return primary,pairdev,primsap,region,timezone 

def process(data):
    print('hello',data.head())
    data_left=data[data['primary']] #keeps primaries
    data_right=data[~data['primary']] #keeps secondaries
    #columns=['source','metricName','apn','device','pair','sap','region','metricTime','locTime','metricValue']
    combined=data_left.merge(data_right,how='outer',left_on=['source','metricName','apn','device','sap','region','metricTime','locTime'],right_on=['source','metricName','apn','pair','sap','region','metricTime','locTime'],indicator=True)
    print(combined.groupby(['_merge']).agg({'metricName':len}))
    (combined[combined['_merge']!='both']).to_csv('check.csv')
    combined=combined[(combined['_merge']=='both') | (combined['metricValue_x']>0) | (combined['metricValue_y']>0)].reset_index(drop=True)
    acting=[]
    metricValue=[]
    for i in range(len(combined.index)):
        if(combined['_merge'].iloc[i]=='both'):
            if(combined.iloc[i]['metricValue_x']<combined.iloc[i]['metricValue_y']):
                acting.append(combined.iloc[i]['device_y'])
                metricValue.append(combined.iloc[i]['metricValue_y'])
            else:#strict equality
                acting.append(combined.iloc[i]['device_x'])
                metricValue.append(combined.iloc[i]['metricValue_x'])
        elif(combined['_merge'].iloc[i]=='left_only' and combined['metricValue_x'].iloc[i]>0):
            acting.append(combined.iloc[i]['device_x'])
            metricValue.append(combined.iloc[i]['metricValue_x'])
        elif(combined['_merge'].iloc[i]=='right_only' and combined['metricValue_y'].iloc[i]>0):
            acting.append(combined.iloc[i]['device_y'])
            metricValue.append(combined.iloc[i]['metricValue_y'])
    combined.insert(3,column='device',value=acting)
    combined['metricValue']=metricValue
    combined=combined.drop(['device_x','pair_x','primary_x','metricValue_x','device_y','pair_y','primary_y','metricValue_y','_merge'],axis=1)
    #print( list(combined.itertuples(index=False,name=None))[0])
    return list(combined.itertuples(index=False,name=None))
