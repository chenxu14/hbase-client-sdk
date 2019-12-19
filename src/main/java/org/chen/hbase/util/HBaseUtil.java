package org.chen.hbase.util;

public class HBaseUtil {
  static int DJBHash(String rowkey) {
    int hash = 5381;
    for (int i = 0; i < rowkey.length(); i++) {
      hash = ((hash << 5) + hash) + rowkey.charAt(i);
    }
    return (hash & 0x7FFFFFFF);
  }

  /**
   * append a hash prefix to the originalRowkey <br>
   * for eample, if the originalRowkey is '<strong>ORDER_87638601</strong>_20180630',
   * if you want to add a hash prefix based on '<strong>ORDER_87638601</strong>', 
   * the real rowkey will be getHashPrefix('<strong>ORDER_87638601</strong>')_originalRowkey
   */
  public static String getHashPrefix(String info) {
    String prefix = String.format("%04d", DJBHash(info) % 10000);
    return prefix;
  }

  /**
   * hash prefix same as hive's GenericUDFHash <br>
   * SELECT lpad(cast(abs(hash(orderId)%10000) as string),4,'0') hash_orderId,orderId
   */
  public static String hiveHashPrefix(String... infos) {
    int hashCode = 0;
    for(String info : infos){
      int fieldHash = 0;
      byte[] infoBytes = info.getBytes();
      for (int i = 0; i < infoBytes.length; i++) {
        fieldHash = fieldHash * 31 + infoBytes[i];
      }
      hashCode = 31 * hashCode + fieldHash;
    }
        String prefix = String.format("%04d", Math.abs(hashCode % 10000));
        return prefix;
  }

  /**
   * prefix a slat value to the orginalRowkey
   */
  public static String getSaltPrefixRowkey(String orginalRowkey) {
    String prefix = String.format("%02d", DJBHash(orginalRowkey) % 100);
    return prefix + MyConstants.SALT_DELIMITER + orginalRowkey;
  }

  /**
   * reverse the target rowkey
   */
  public static String reverse(String info) {
    return new StringBuilder(info).reverse().toString();
  }

  public static void main(String[] args) {
    String rowkey = "ORDER_87638601_20180630";
    String prefix = HBaseUtil.getHashPrefix("ORDER_87638601");
    System.out.println("orginal rowkey : " + rowkey + ", real rowkey : " + prefix + "_" + rowkey);
    System.out.println(hiveHashPrefix("101404156548579741337610000010"));
    System.out.println(reverse("abcde"));
    System.out.println(getSaltPrefixRowkey("abcde_" + System.currentTimeMillis()));
    
    String s = "";
    System.out.println(s.substring(0,0));
  }
}
