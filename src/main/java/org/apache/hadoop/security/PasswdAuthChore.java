package org.apache.hadoop.security;

import static org.apache.hadoop.util.PlatformName.IBM_JAVA;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import org.chen.hbase.client.HBaseWriteClient;
import org.chen.hbase.util.MyConstants;

public class PasswdAuthChore extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(HBaseWriteClient.class);
  private String principal;
  private String password;
  private LoginContext loginContext;
  private User ugiUser;
  private static boolean shouldRenewImmediatelyForTests = false;

  public PasswdAuthChore(Configuration conf, Stoppable stoppable, int period) throws IOException {
    super("HBaseAuthChore", stoppable, period, period);
    this.principal = conf.get("hbase.client.kerberos.principal", MyConstants.DEFAULT_PRINCIPAL);
    this.password = conf.get("hbase.client.keytab.password");
    try {
      Subject subject = new Subject();
      loginContext = new LoginContext("HBaseClientAuth", subject, initHandler(), new HBaseAuthConfiguration());
      loginContext.login();
      if (subject.getPrincipals(KerberosPrincipal.class).isEmpty()) {
        throw new IOException("Provided Subject must contain a KerberosPrincipal");
      }
      KerberosPrincipal principal = subject.getPrincipals(KerberosPrincipal.class).iterator().next();
      ugiUser = new User(principal.getName(), AuthenticationMethod.KERBEROS, loginContext);
      subject.getPrincipals().add(ugiUser);
      ugiUser.setLastLogin(Time.now());
      UserGroupInformation loginUser = new HBaseUGI(subject);
      UserGroupInformation.setLoginUser(loginUser);
      LOG.info("successfully login for " + principal + " by passwd!");
    } catch (LoginException le) {
      throw new IOException("Login failure for " + principal + " with passwd " + password + ": " + le, le);
    }
  }

  @Override
  protected void chore() {
    try {
      UserGroupInformation.getLoginUser().reloginFromTicketCache();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  class HBaseUGI extends UserGroupInformation {
    HBaseUGI(Subject subject) {
      super(subject);
    }
    @Override
    public synchronized void reloginFromTicketCache() throws IOException {
      LOG.info("execute relogin from TicketCache for" + principal + " by passwd!");
      try {
        Set<KerberosTicket> tickets = getSubject().getPrivateCredentials(KerberosTicket.class);
        KerberosTicket tgt = null;
        for (KerberosTicket ticket : tickets) {
          if (SecurityUtil.isOriginalTGT(ticket)) {
              tgt = ticket;
              break;
          }
        }
        if(tgt != null){
          long start = tgt.getStartTime().getTime();
          long end = tgt.getEndTime().getTime();
          long expire = start + (long) ((end - start) * 0.8);
          if(Time.now() < expire && !shouldRenewImmediatelyForTests){
            LOG.info("next refresh time is " + expire + ",skip relogin");
            return;
          }
        }
        loginContext.logout();
        loginContext = new LoginContext("CloudTableAuth", getSubject(), initHandler(),
            new HBaseAuthConfiguration());
        loginContext.login();
        ugiUser.setLastLogin(Time.now());
        LOG.info("successfully relogin for " + principal + " by passwd!");
      } catch (LoginException e) {
        throw new IOException("Login failure for " + principal + " with passwd " + password, e);
      }
    }
  }

  private static class HBaseAuthConfiguration extends javax.security.auth.login.Configuration {
    private static final Map<String, String> USER_KERBEROS_OPTIONS = new HashMap<String, String>();
    static {
      if (IBM_JAVA) {
        USER_KERBEROS_OPTIONS.put("useDefaultCcache", "true");
      } else {
        USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
      }
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        if (IBM_JAVA) {
          System.setProperty("KRB5CCNAME", ticketCache);
        } else {
          USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
        }
      }
      USER_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
      USER_KERBEROS_OPTIONS.put("renewTGT", "true");
      USER_KERBEROS_OPTIONS.put("doNotPrompt", "false");
      String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
      if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
        USER_KERBEROS_OPTIONS.put("debug", "true");
      }
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      return new AppConfigurationEntry[] { new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
          LoginModuleControlFlag.OPTIONAL, USER_KERBEROS_OPTIONS) };
    }

  }

  private CallbackHandler initHandler() {
    return new CallbackHandler() {
      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
          if (callbacks[i] instanceof TextOutputCallback) {
            TextOutputCallback tc = (TextOutputCallback) callbacks[i];
            String text;
            switch (tc.getMessageType()) {
            case TextOutputCallback.INFORMATION:
              text = "[INFO]";
              break;
            case TextOutputCallback.WARNING:
              text = "[WARN] ";
              break;
            case TextOutputCallback.ERROR:
              text = "[ERROR]";
              break;
            default:
              throw new UnsupportedCallbackException(callbacks[i], "Unrecognized message type");
            }
            String message = tc.getMessage();
            if (message != null) {
              LOG.info(text + message);
            }
          } else if (callbacks[i] instanceof NameCallback) {
            NameCallback nc = (NameCallback) callbacks[i];
            nc.setName(principal);
          } else if (callbacks[i] instanceof PasswordCallback) {
            PasswordCallback pc = (PasswordCallback) callbacks[i];
            pc.setPassword(password.toCharArray());
          } else {
            throw new UnsupportedCallbackException(callbacks[i],
                "Unrecognized Callback:" + callbacks[i].getClass());
          }
        }
      }
    };
  }

  @VisibleForTesting
  static void setShouldRenewImmediatelyForTests(boolean immediate) {
    shouldRenewImmediatelyForTests = immediate;
  }
}
