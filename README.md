# 博客來首頁爬蟲

> __腳本__ : 
> - Script_Homepage.py
> - ETL_Pivot_Homepage.py

> __程式__ : 
> - Crawler/utils.py (爬蟲階段function)
> - Crawler/ETL_utils.py (ETL階段function)

> __資料__ : 
> - origin_data_202207142000.csv (爬蟲結果原始資料)
> - ETL_result/home_page_main_df.csv (經邏輯處理的觀察資料Group_main)
> - ETL_result/home_page_sub_df.csv (經邏輯處理的觀察資料Group_sub)

> __參數檔__ : 
> - target_schema/group_mapping.csv (ETL時各網頁的群組分類)
> - parameters.csv (爬蟲時的相關設定)


## 腳本說明 
>### Script_Homepage.py :
> 整理HTML BODY下所有XPATH，並找出 __['div','li','ui','span']__ 結尾的xpath，再以腳本使滑鼠移至element位置取得更多資料及指定xpath，最後將完整body xpath寫入cassandra
>
> __原始資料 (指定群組)__
>
> |insert_time |xpath |datatype                |group_lv1|group_lv2                                                 |sub_xpath|value    |
> |------------|------|------------------------|---------|----------------------------------------------------------|---------|---------|
> |202207142000|div[4]/div/div[8]/div/ul/li[2]/div|class                   |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[2]/div|         |
> |202207142000|div[4]/div/div[8]/div/ul/li[2]/div|text                    |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[2]/div|正官庄      |
> |202207142000|div[4]/div/div[8]/div/ul/li[3]/div|class                   |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[3]/div|         |
> |202207142000|div[4]/div/div[8]/div/ul/li[3]/div|text                    |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[3]/div|毛孩放暑假    |
> |202207142000|div[4]/div/div[8]/div/ul/li[4]/div|class                   |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[4]/div|         |
> |202207142000|div[4]/div/div[8]/div/ul/li[4]/div|text                    |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[4]/div|夏季動漫展    |
> |202207142000|div[4]/div/div[8]/div/ul/li[5]/div|class                   |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[5]/div|         |
> |202207142000|div[4]/div/div[8]/div/ul/li[5]/div|text                    |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[5]/div|夏酒菜特輯    |
> |202207142000|div[4]/div/div[8]/div/ul/li[6]/div|class                   |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[6]/div|         |
> |202207142000|div[4]/div/div[8]/div/ul/li[6]/div|text                    |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[6]/div|貓奇幻樂園    |
> |202207142000|div[4]/div/div[8]/div/ul/li[7]/div|class                   |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[7]/div|         |
> |202207142000|div[4]/div/div[8]/div/ul/li[7]/div|text                    |div[4]/div/div[8]/div|div[4]/div/div[8]/div/ul                                  |/li[7]/div|字我訂造展    |


> ### ETL_Pivot_Homepage.py :
> 以整體彈性部份客製方式(無論新增多少活動組別，透過程式邏輯會分類至同組，出現新組別會寫入指定table並提示新組別未被給予分類名稱)，並將text值回推至群組名稱，產出Pivot Table
>
> __經ETL處理後__
>
> |insert_time |pivot_class|group_lv2               |key_pg2|href                                                      |text   |url      |
> |------------|-----------|------------------------|-------|----------------------------------------------------------|-------|---------|
> |202207142000|創意生活_1     |div[4]/div/div[8]/div/ul|1      |https://activity.books.com.tw/crosscat/show/A00000052250&#124;&#124;|正官庄&#124;&#124;  |home_page|
> |202207142000|創意生活_2     |div[4]/div/div[8]/div/ul|1      |https://activity.books.com.tw/crosscat/show/A00000051945&#124;&#124;|毛孩放暑假&#124;&#124;|home_page|
> |202207142000|創意生活_3     |div[4]/div/div[8]/div/ul|1      |https://activity.books.com.tw/crosscat/show/A00000052056&#124;&#124;|夏季動漫展&#124;&#124;|home_page|
> |202207142000|創意生活_4     |div[4]/div/div[8]/div/ul|1      |https://activity.books.com.tw/crosscat/show/A00000051361&#124;&#124;|夏酒菜特輯&#124;&#124;|home_page|
> |202207142000|創意生活_5     |div[4]/div/div[8]/div/ul|1      |https://activity.books.com.tw/crosscat/show/A00000051655&#124;&#124;|貓奇幻樂園&#124;&#124;|home_page|

## 參數設定
> ### group_mapping.csv 說明 :
> 以csv方式存取，不用DB方式原因為資料量小且容易變動
>
> 目前只開發首頁，若接續開發也將延用此方式擴充爬蟲標的的群組分類
>
>|group_lv1            |group_lv2                                 |class_name|
>|---------------------|------------------------------------------|----------|
>|div[3]/div/div[2]    |                                          |Banner_1  |
>|div[3]/div/div[3]/div|                                          |活動_摺疊_最上方 |
>|                     |div[3]/div/div[6]/div/div/div[2]/ul/li/   |全站分類_1    |
>|                     |div[3]/div/div[6]/div/div/div[2]/ul/li[2]/|全站分類_2    |
>|                     |div[3]/div/div[6]/div/div/div[2]/ul/li[3]/|全站分類_3    |
>

> ### parameters.csv 說明 :
>
> 以字典型態儲存爬蟲相關設定
> 
>> __get_data_type_list__ : 個別xpath結尾的node儲存的屬性
>>
>> __table_param_dict__ : 以pattern方式尋找符合的url下會依參數設定取得`table name , table columns (含xpath取不到的log table)`
>>
>> __剩餘參數__ : 各function執行時各情境下打印文字
