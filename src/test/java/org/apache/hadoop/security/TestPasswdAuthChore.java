/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;

public class TestPasswdAuthChore {
  private static final String principal = "foo";
  private static final String passwd = "test123";
  private MiniKdc kdc;
  private File workDir;
  private Configuration conf;

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void startMiniKdc() throws Exception {
    conf = new Configuration();
    conf.set("hbase.security.authentication", "kerberos");
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hbase.security.authorization", "true");
    conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
    conf.set("hbase.client.kerberos.principal", principal);
    conf.set("hbase.client.keytab.password", passwd);
    conf.set("hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1]");
    UserGroupInformation.setConfiguration(conf);
    workDir = folder.getRoot();
    kdc = new MiniKdc(MiniKdc.createConf(), workDir);
    kdc.start();
    kdc.createPrincipal(principal, passwd);
  }

  @After
  public void stopMiniKdc() {
    if (kdc != null) {
      kdc.stop();
    }
  }

  @Test
  public void testUGILoginFromPasswd() throws Exception {
    PasswdAuthChore auth = new PasswdAuthChore(conf, null, 3600);
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    // testShouldAuthenticateOverKrb
    Assert.assertTrue("Security should enabled!", org.apache.hadoop.hbase.security.User.isHBaseSecurityEnabled(conf));
    Assert.assertTrue("loginUser should not be null!", loginUser != null);
    Assert.assertTrue("loginUser should has Kerberos TGT!", loginUser.hasKerberosCredentials());
    Assert.assertTrue("loginUser should equals currentUser!", loginUser.equals(currentUser));
    // verify relogin with passwd.
    User user = loginUser.getSubject().getPrincipals(User.class).iterator().next();
    long firstLogin = user.getLastLogin();
    PasswdAuthChore.setShouldRenewImmediatelyForTests(true);
    auth.chore();
    long lastLogin = user.getLastLogin();
    Assert.assertTrue("User should have been able to relogin from keytab",
        lastLogin > firstLogin);
  }

}
