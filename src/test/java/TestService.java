import com.owdp.dbutil.DbUtils;

/**
 * Created by user on 2017/4/17.
 */
public class TestService {
    public static void main(String[] args) {
        TestService service1 = DbUtils.getServiceImpl(TestService.class);
        TestService service2 = DbUtils.getServiceImpl(TestService.class);

        System.out.println(service1 == service2); //true。service是单例多线程模型的


    }
}
