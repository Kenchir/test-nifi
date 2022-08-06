package safaricomet.bigdata.nifi;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import static org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers.x509Certificate;


@SpringBootApplication
@EnableAutoConfiguration
public class NifiApplication {

	public static void main(String[] args) {
		SpringApplication.run(NifiApplication.class, args);
	}
	@Bean
	public ClientHttpRequestFactory createRequestFactory() {
		PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
		connectionManager.setMaxTotal(1000);
		connectionManager.setDefaultMaxPerRoute(10);

		RequestConfig config = RequestConfig.custom().setConnectTimeout(100000).build();
		CloseableHttpClient httpClient = HttpClientBuilder.create().setConnectionManager(connectionManager)
				.setDefaultRequestConfig(config).build();
		return new HttpComponentsClientHttpRequestFactory(httpClient);
	}
	@Bean
	public RestTemplate restTemplate()
			throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
		TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;

		SSLConnectionSocketFactory scsf = new SSLConnectionSocketFactory(
				SSLContexts.custom().loadTrustMaterial(null,acceptingTrustStrategy).build(),
				NoopHostnameVerifier.INSTANCE);

		CloseableHttpClient httpClient = HttpClients.custom()
				.setSSLSocketFactory(scsf)
				.disableCookieManagement()
				.build();

		HttpComponentsClientHttpRequestFactory requestFactory =
				new HttpComponentsClientHttpRequestFactory();

		requestFactory.setHttpClient(httpClient);


		HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
		return new RestTemplate(requestFactory);
	}
}
