package com.hbgj.hive.util;


import com.hbgj.code.CodeUtil;

public class Test {

    public static void  main(String[] args) throws Exception {
        String code=CodeUtil.encode("hadoop");

        String rs=CodeUtil.decode("늘복앞난킨했향본주국우밀으원쳐규엘쳐불국규지간두근렇세센높가감난온정나씨법스");

        System.out.println(code+"|"+rs);

    }
}
