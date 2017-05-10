package com.urv.blackeagle.function.signaturevalidator;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;


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
        	HttpURLConnection pubkey = api.swift.get("rsa_public_keys/gerard.pub");
            InputStream keyfis = pubkey.getInputStream();
            byte[] encKey = new byte[keyfis.available()];  
            keyfis.read(encKey);
            keyfis.close();
            	
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(encKey);

            KeyFactory keyFactory = KeyFactory.getInstance("DSA", "SUN");
            PublicKey pubKey = keyFactory.generatePublic(pubKeySpec);

            /* input the signature bytes */
            String base64Sig = ctx.object.metadata.get("Signature");
            byte[] sigToVerify = Base64.getDecoder().decode(base64Sig);

            /* create a Signature object and initialize it with the public key */
            Signature sig = Signature.getInstance("SHA1withDSA", "SUN");
            sig.initVerify(pubKey);

            /* Update and verify the data */
            byte[] buffer;
            while((buffer = ctx.object.stream.readBytes()) != null) {
                sig.update(buffer);
            };

            verifies = sig.verify(sigToVerify);

            ctx.log.emit("signature verifies: " + verifies);

        } catch (Exception e) {
        	ctx.object.stream.write("Error validating the object");
        }
        
        ctx.object.stream.write("Signature verifies: " + verifies + "\n");
		
		ctx.log.emit("Ended Signature Verification Function");

	}
	
}
