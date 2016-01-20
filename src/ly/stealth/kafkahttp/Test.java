package ly.stealth.kafkahttp;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * User: yanbit
 * Date: 2016/1/20
 * Time: 10:47
 */
public class Test {
    public static void main(String[] args){
        String s = "{\"name\":\"Zara\", \"shelf\":5}";

        // 使用json-simple中的parser
        JSONParser parser=new JSONParser();
        JSONObject json ;
        try {
            json= (JSONObject) parser.parse(s);
            json.put("age","15");
            System.out.println(json.toJSONString());
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
