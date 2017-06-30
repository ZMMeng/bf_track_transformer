package com.beifeng.etl.utils;

import org.junit.Test;

/**
 * Created by Administrator on 2017/6/30.
 */
public class TestUserAgentUtil {

    @Test
    public void testAnalyticsUserAgent() {
        String userAgent = null;
        userAgent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36";
        userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0";
        userAgent = "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E)";
        UserAgentUtil.UserAgentInfo result = UserAgentUtil.analyticsUserAgent(userAgent);
        System.out.println(result);
    }
}
