import requests

# url = "https://www.nike.com/kr/w/women-shoes-5e1x6zy7ok"
# parameter = {
#     "queryid": "filteredProductsWithContext",
#     "anonymousId": "A752FB5CBFBCAE92B90EA25EFCD46723",
#     "uuids": "7baf216c-acc6-4452-9e07-39c2ca77ba32,de314d73-b9c5-4b15-93dd-0bef5257d3b4",
#     "language": "ko",
#     "country": "KR",
#     "channel": "NIKE",
#     "localizedRangeStr": "{lowestPrice} ~ {highestPrice}",
#     "path": "/kr/w/women-tops-tshirts-5e1x6z9om13",
# }


url = "https://api.nike.com/cic/browse/v2"
parameter = {
"queryid": "products",
"anonymousId": "A752FB5CBFBCAE92B90EA25EFCD46723",
"country": 'kr',
"endpoint": "/product_feed/rollup_threads/v2?filter=marketplace(KR)&filter=language(ko)&filter=employeePrice(true)&filter=attributeIds(a00f0bb2-648b-4853-9559-4cd943b7d6c6,7baf216c-acc6-4452-9e07-39c2ca77ba32)&anchor=144&consumerChannelId=d9a5bc42-4b9c-4976-858a-f159cf99c647&count=24",
"language": "ko",
"localizedRangeStr": "{lowestPrice} ~ {highestPrice}"
}
# todo : anchor 가 시작인듯 24개씩 증가하는데 ??

headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cookie": 'geoloc=cc=KR,rc=,tp=vhigh,tz=GMT+9,la=37.57,lo=127.00; anonymousId=A752FB5CBFBCAE92B90EA25EFCD46723; s_ecid=MCMID%7C82915684168003994746993304328694580623; AMCVS_F0935E09512D2C270A490D4D%40AdobeOrg=1; AMCV_F0935E09512D2C270A490D4D%40AdobeOrg=1994364360%7CMCMID%7C82915684168003994746993304328694580623%7CMCAID%7CNONE%7CMCOPTOUT-1697043181s%7CNONE%7CvVersion%7C3.4.0; AnalysisUserId=8e436f6e-1f49-4294-a158-ed38635d88f3; CONSUMERCHOICE=kr/ko_kr; NIKE_COMMERCE_COUNTRY=KR; NIKE_COMMERCE_LANG_LOCALE=ko_KR; _gcl_au=1.1.1032900399.1697036070; _gid=GA1.2.1571996457.1697036071; _fbp=fb.1.1697036070835.970349052; nike1_CID=0ca79940af3c4790c1e85649d2b3145b; nike1_FIRSTDATE=1697036070989; _gac_UA-167630499-4=1.1697097563.Cj0KCQjwj5mpBhDJARIsAOVjBdpFYE2gzscLPcTekzgyvld3s58iSyJKlMfak8zg3HIV0IbdIUQ8-gEaAu5sEALw_wcB; nike1_LASTDATE=1697097563016; _gcl_aw=GCL.1697097563.Cj0KCQjwj5mpBhDJARIsAOVjBdpFYE2gzscLPcTekzgyvld3s58iSyJKlMfak8zg3HIV0IbdIUQ8-gEaAu5sEALw_wcB; bm_mi=0D6E16C8A2FEB6B9655BCC57D3A94C53~YAAQdHkyF0u4tf+KAQAAUWZRJBVpdFr5ZhZY+GRdOas6LNiJynKSWB/MKKJzGXgwZJclX2niRrZUSzkCaz5/Sxz81Q55UccNfB9RlRrHseADLHb4+H49g4SVPHpAYbTpiWlPZZ2+1Evvdt4rIR3SJJ+sBePcLdY9wiYuEnWDL0XS94HazFmtsmO7DC8gEQ4MbSHmOEJFupO10gSQZOZ+pim0uAxRt/v/tQ2uwjpZCANrlbZmTJyygnd4BhbwuHhoRmOEOKJMr9/it1l5f7xppU6loE0L5H4nsgUQMFpxko++5qezqDQaYLC9InPV+HAI26T3HmAHPy0vZDZxUHVwJ06jS0Pi3Kh4J2YR1e/20EOQrA==~1; ak_bmsc=556CCE37C8481D11574C6F75A3F3D2FE~000000000000000000000000000000~YAAQdHkyFy33tf+KAQAA5ptRJBUWou+2WE7OJ/YzyR/cGCl2/2vzamD1ntc0HUt3GNitPLJ48o+hohnMVKHiyKOHqh3T1kXVcy09xwXDv0yUqxl+fKdlvCeh+BtZS0MzClX5AxQ8kyl3kzG1M70GLWHcHEnZBKBHOtjte4iNsHq9qkcD58jq1baOOoO+teQ6OtEM2/wFMNk7+9Tudah25aSsmMxeHVcKrOaBpzr+Gqi9kKfNnJXFYwmgsAFaJXo7C3nLFLYTnJb2PC/6zyfDbrR2+k170vKiHKSg02sNTFULOU2ZeX6A47P6cDrgQZdDxzTLajTwBo+OkZDd0KHHBGRXYwPIOKmCJlUtAtybwcZT2DV41nNttJi/AkvjSsqKjAdLLMjMKBhZUjN9G5ckOylCMnXzD1iITLMshL4lIva0o9iCZTtufSLiBirubuPwzh1q2evSKLstp8WakGldicRPRH3oz8c8FpFlw1hbr/GMv9dC0pWvEzcD74b6BmqaJlWaHeuoXkVyjTO5G1TmbuXkAL8X/uyQs3wyk+PaTVlPiIu9KzgR9Q==; audience_segmentation_performed=true; bm_sz=CE7533062A653358ABB2BCE5C594B348~YAAQl+Q1F9b12SGLAQAAQXF+JBUUz1pDkcXEnuDT/vphVDeLerF/9bTrmfnxZEsNEH5dn1U+MAae2ZpCQT3Fv9Bwv2qR7VyFXZAc2/24mpvPiY8LgQQHDmfrBsMdFE14FMcUG7WhKBti+WCri/639dEBVU4vq74VtoC7qv4uawPups70S2W2VtaHcFP1unzPUkd4wsc5nB3vnfeWM6lgkBwWmsSoXU7lR4poYp6oOCD5DdcYi6LJDQnHKTjiLw3HjWWHfLn1y7UlSqQjnX3h3WeQRxGCjlFLPSZKd3q37whhDBBIe5+N2LfzHiuNHRMCsbZUS3brospqfXT9csGpQHx5yaM6L4eC3DaA4wIpcMVSj+Uh22gSrts7qcltbZcSxabQ/VJ7p5sTxo63xLq155pG+nV9UXDmNqn/vPGmNSNGSLGk6mtw8BUZ8x9LxSn/YFvLvgnmNOt7WTdyHETJ/HjxCGuC~3289651~4339250; forterToken=393825e67afa4caa8890bc0e3564796f_1697124347646__UDF43_19ck; ftr_blst_1h=1697124348299; ppd=pw|nikecom>pw>women_clothing; AKA_A2=A; ak_bmsc_nke-2.2-ssn=01MrF4mtrw00jVLInqRLqksNnK2QSqirQ3Ip43z73rnag3W2rCfjn1KlD3jxwu9E29i7vpN98tigVKxgDnAY1Rzxat6MvwqMAm5szdTmI2zhX7g8tIm4MirS5PUolUME3UGrKbMUUjhzcoCBe0qkLKvRyh9qSh; ak_bmsc_nke-2.2=01MrF4mtrw00jVLInqRLqksNnK2QSqirQ3Ip43z73rnag3W2rCfjn1KlD3jxwu9E29i7vpN98tigVKxgDnAY1Rzxat6MvwqMAm5szdTmI2zhX7g8tIm4MirS5PUolUME3UGrKbMUUjhzcoCBe0qkLKvRyh9qSh; bm_sv=6D8F8814CF5587FFEC4043E21AD1FCE6~YAAQ4zpvPaoWoxyLAQAADtOVJBXFQN2/g3XCKuFzzqtjc+vStPiEbbxOeb1fSMPfz2fLNNpUD4naewUhMV1R2PiUpsyznXxKmdvdg6iNFNm709R/XVNairHWEdoFT/37uiSqJLXGp02Hndzlv3DLHUhHAH7UizqSiFpNcSdUWQu6jhHUAKVrYNj/srCzYwjGOgieGogxgBts8PuRtpzd6pABlzXV3eHODSkgjwlKOlKPW7/88BzQq+NICfrtBPw=~1; _abck=DAA68A0CFB0008700D5D798BF632B137~-1~YAAQJXwhF2pf+/CKAQAAY9WVJArjjmI0r/+QLZXbfm87ivrTCsLFQx1kbFCuqcvcB5kzE5nl8U63+KLR+GqtNPvt3VRVGqHPw5kUehJpPfhmTZxK3wW9prhXesut1p3Th461q7XMxp7PhozMLBZ0MeE9aNuGhAGfyWyU1S6qOaJIGhgmAbew06FgdVFdBjPgqaoIlZgqC2cTLM7DedpNRGkPJKp55B2lTEE8faKYjm6zJZmmS/8lOFT/Td4ljQd0zVizPPszCB4TiT9IQUJfrEgT7OfYhjsAEvR7fZP89YmzgmYaV6Gu7Lwn4Eh6FewhOrUifdHNjoZjJ6wGfpLLYFF9ERbotRFCPOkii2olPp5L8jubXO07PJWqlpDvFq5t+o1pT+7wNwNouj48uLHoSdpZn+LzzgtHP8ZZKNBVZlzGTAUWzRsOUefW7hZJLhLWlV5xEKO+bjtYXunMVxKSIcn3rOb7v8ROJK6FGFngkGCKyjDXW8PIQDnyVI80om7Uewd+tULq7JY=~-1~-1~-1; _uetsid=1558b9a0684611ee9691e36f870ae584; _uetvid=15589f60684611ee86ca29510942800d; _ga_QTVTHYLBQS=GS1.1.1697124181.5.1.1697125881.0.0.0; _ga=GA1.1.957175052.1697036070; _ga_Q142L8EL30=GS1.1.1697124181.5.1.1697125881.56.0.0; cto_bundle=5vHdaV85RlRWNE15NVFWQjJmQmZiV0o3cGtpdDRiRTlTYWRuNHVsMSUyQkNEV2ZhMWxqJTJGUlBpMGhzSzRUb1ZrRG0xJTJGTnpTQ3JZbzlxRW13R2I4aUQlMkJVWWdSQmo0eDRmSWxGV1NOTmk5TVlNV3cxRmVYRlFHbWs0OFluWHM0aUxMbllkbVdycGROc1hJV2QzOGtjJTJGM1h2RkpJZ24yNjZETGZZJTJGT1h5b2plNlVhNiUyRjBjSWxiV1IzenpicXpBdFdrbXFibzJuJTJCd0ZERGtKenlVQ2txbWJJZFNrRjloUSUzRCUzRA; RT="z=1&dm=nike.com&si=dcb59d1b-ae2d-4b31-937e-de7e9eea6317&ss=lnnbx3i0&sl=7&tt=50l&bcn=%2F%2F684d0d47.akstat.io%2F&nu=1ozaakmg&cl=19pbw"',
    "Host": "api.nike.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
}

def scraping():
    resp = requests.get(url, params=parameter, headers=headers)
    print("resp: ", resp.json())
    return resp


if __name__ == "__main__":
    scraping()

    """  
    Connection:
    keep-alive 
    Origin:
    https://www.nike.com
    Referer:
    https://www.nike.com/
    Sec-Ch-Ua:
    "Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"
    Sec-Ch-Ua-Mobile:
    ?0
    Sec-Ch-Ua-Platform:
    "macOS"
    Sec-Fetch-Dest:
    empty
    Sec-Fetch-Mode:
    cors
    Sec-Fetch-Site:
    same-site
    :

        """