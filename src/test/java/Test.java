import java.lang.reflect.Field;

public class Test {
    public static void main(String[] args) throws Exception {
        Person p = new Person();
        Class clazz = p.getClass();

        Field field = clazz.getDeclaredField("a_name");
        field.setAccessible(true);
        //field.set(p,"aaaa");

        String aa = (String) field.get(p);

        System.out.println(aa);
        System.out.println(p);
    }
}
