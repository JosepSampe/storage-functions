package com.urv.blackeagle.function.signaturevalidator;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.UUID;


/**
 * 
 * @author Gerard Paris
 *
 */

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.log.emit("Init Signature Verification Function");
		
		boolean verifies = false;

        try {
        	/* import encoded public key */
        	ctx.log.emit("Getting public key");
        	HttpURLConnection pubkey = api.swift.get("rsa_public_keys/gerard.pub");
            InputStream keyfis = pubkey.getInputStream();
            byte[] encKey = new byte[keyfis.available()];  
            keyfis.read(encKey);
            keyfis.close();
            	
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(encKey);

            KeyFactory keyFactory = KeyFactory.getInstance("DSA", "SUN");
            PublicKey pubKey = keyFactory.generatePublic(pubKeySpec);

            /* input the signature bytes */
            ctx.log.emit("Getting signature from object metadata");
            String base64Sig = ctx.object.metadata.get("Signature");
            byte[] sigToVerify = Base64.getDecoder().decode(base64Sig);

            /* create a Signature object and initialize it with the public key */
            Signature sig = Signature.getInstance("SHA1withDSA", "SUN");
            sig.initVerify(pubKey);

            /* Update and verify the data, store the object locally */
            ctx.log.emit("Verifying signature");
            String uuid = UUID.randomUUID().toString();
			String tmp_file = "/tmp/"+uuid;
			byte[] buffer;
            FileOutputStream fos = new FileOutputStream(tmp_file);
            while((buffer = ctx.object.stream.readBytes()) != null) {
                sig.update(buffer);
                fos.write(buffer);
            };
            fos.close();          

            verifies = sig.verify(sigToVerify);

            ctx.log.emit("signature verifies: " + verifies);
            
            
            File file = new File(tmp_file);
            
            if (verifies){
            	FileInputStream ios = new FileInputStream(file);
                int len = 0;
                int bytes = 65535;
                buffer = new byte[bytes];
                while ((len = ios.read(buffer)) != -1) {
                	
                	if (len != bytes) {
        				byte[] smallerData = new byte[len];
        		        System.arraycopy(buffer, 0, smallerData, 0, len);
        		        buffer = smallerData;
        		    }
                	
                	ctx.object.stream.writeBytes(buffer);
                }
                ios.close();            	
            } else {
            	ctx.object.stream.write("Error: Invalid Signature");
            }
            
            file.delete();           
            

        } catch (Exception e) {
        	ctx.object.stream.write("Error validating the object");
        }
		
		ctx.log.emit("Ended Signature Verification Function");

	}
	
}
