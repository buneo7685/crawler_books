# 博客來首頁爬蟲
## 說明 
>### Stage 1 :
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


> ### Stage 2 :
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
