### **TargetTc**

curl 'https://www.jma.go.jp/bosai/typhoon/data/targetTc.json' \\

&#x20; -H 'accept: \*/\*' \\

&#x20; -H 'accept-language: vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5' \\

&#x20; -H 'if-modified-since: Tue, 21 Apr 2026 00:02:29 GMT' \\

&#x20; -H 'if-none-match: "d751713988987e9331980363e24189ce"' \\

&#x20; -H 'priority: u=1, i' \\

&#x20; -H 'referer: https://www.jma.go.jp/bosai/map.html' \\

&#x20; -H 'sec-ch-ua: "Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"' \\

&#x20; -H 'sec-ch-ua-mobile: ?0' \\

&#x20; -H 'sec-ch-ua-platform: "Windows"' \\

&#x20; -H 'sec-fetch-dest: empty' \\

&#x20; -H 'sec-fetch-mode: cors' \\

&#x20; -H 'sec-fetch-site: same-origin' \\

&#x20; -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36' \\

&#x20; -H 'x-requested-with: XMLHttpRequest'

### 

### **Target Times**

curl 'https://www.jma.go.jp/bosai/typhoon/data/prob50kt/targetTimes.json' \\

&#x20; -H 'accept: \*/\*' \\

&#x20; -H 'accept-language: vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5' \\

&#x20; -H 'if-modified-since: Mon, 20 Apr 2026 00:41:34 GMT' \\

&#x20; -H 'if-none-match: "d751713988987e9331980363e24189ce"' \\

&#x20; -H 'priority: u=1, i' \\

&#x20; -H 'referer: https://www.jma.go.jp/bosai/map.html' \\

&#x20; -H 'sec-ch-ua: "Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"' \\

&#x20; -H 'sec-ch-ua-mobile: ?0' \\

&#x20; -H 'sec-ch-ua-platform: "Windows"' \\

&#x20; -H 'sec-fetch-dest: empty' \\

&#x20; -H 'sec-fetch-mode: cors' \\

&#x20; -H 'sec-fetch-site: same-origin' \\

&#x20; -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36' \\

&#x20; -H 'x-requested-with: XMLHttpRequest'

### 

### **PastTracks**

curl 'https://www.jma.go.jp/bosai/typhoon/data/pastTracks.json' \\

&#x20; -H 'accept: \*/\*' \\

&#x20; -H 'accept-language: vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5' \\

&#x20; -H 'if-modified-since: Tue, 21 Apr 2026 00:02:29 GMT' \\

&#x20; -H 'if-none-match: W/"9f9b2cf85bf8b1f57b85ae449a22a5fe"' \\

&#x20; -H 'priority: u=1, i' \\

&#x20; -H 'referer: https://www.jma.go.jp/bosai/map.html' \\

&#x20; -H 'sec-ch-ua: "Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"' \\

&#x20; -H 'sec-ch-ua-mobile: ?0' \\

&#x20; -H 'sec-ch-ua-platform: "Windows"' \\

&#x20; -H 'sec-fetch-dest: empty' \\

&#x20; -H 'sec-fetch-mode: cors' \\

&#x20; -H 'sec-fetch-site: same-origin' \\

&#x20; -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36' \\

&#x20; -H 'x-requested-with: XMLHttpRequest'



## 

## **Convert CURL**

### **TargetTc**

headers = {

&#x20;   'accept': '\*/\*',

&#x20;   'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',

&#x20;   'if-modified-since': 'Tue, 21 Apr 2026 00:02:29 GMT',

&#x20;   'if-none-match': '"d751713988987e9331980363e24189ce"',

&#x20;   'priority': 'u=1, i',

&#x20;   'referer': 'https://www.jma.go.jp/bosai/map.html',

&#x20;   'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',

&#x20;   'sec-ch-ua-mobile': '?0',

&#x20;   'sec-ch-ua-platform': '"Windows"',

&#x20;   'sec-fetch-dest': 'empty',

&#x20;   'sec-fetch-mode': 'cors',

&#x20;   'sec-fetch-site': 'same-origin',

&#x20;   'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',

&#x20;   'x-requested-with': 'XMLHttpRequest',

}

### 

### **Target Times**

headers = {

&#x20;   'accept': '\*/\*',

&#x20;   'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',

&#x20;   'if-modified-since': 'Mon, 20 Apr 2026 00:41:34 GMT',

&#x20;   'if-none-match': '"d751713988987e9331980363e24189ce"',

&#x20;   'priority': 'u=1, i',

&#x20;   'referer': 'https://www.jma.go.jp/bosai/map.html',

&#x20;   'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',

&#x20;   'sec-ch-ua-mobile': '?0',

&#x20;   'sec-ch-ua-platform': '"Windows"',

&#x20;   'sec-fetch-dest': 'empty',

&#x20;   'sec-fetch-mode': 'cors',

&#x20;   'sec-fetch-site': 'same-origin',

&#x20;   'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',

&#x20;   'x-requested-with': 'XMLHttpRequest',

}



### **PastTracks**

headers = {

&#x20;   'accept': '\*/\*',

&#x20;   'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',

&#x20;   'if-modified-since': 'Tue, 21 Apr 2026 00:02:29 GMT',

&#x20;   'if-none-match': 'W/"9f9b2cf85bf8b1f57b85ae449a22a5fe"',

&#x20;   'priority': 'u=1, i',

&#x20;   'referer': 'https://www.jma.go.jp/bosai/map.html',

&#x20;   'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',

&#x20;   'sec-ch-ua-mobile': '?0',

&#x20;   'sec-ch-ua-platform': '"Windows"',

&#x20;   'sec-fetch-dest': 'empty',

&#x20;   'sec-fetch-mode': 'cors',

&#x20;   'sec-fetch-site': 'same-origin',

&#x20;   'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',

&#x20;   'x-requested-with': 'XMLHttpRequest',

}

