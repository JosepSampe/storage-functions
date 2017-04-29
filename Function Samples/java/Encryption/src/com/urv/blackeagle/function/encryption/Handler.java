package com.urv.blackeagle.function.encryption;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	private String encrypt(Context ctx, String data){
		String initVector = "RandomInitVector"; //TODO: Get from external and secure key store, 16 bytes IV
		IvParameterSpec iv = null;
		SecretKeySpec skeySpec = null;
		Cipher cipher = null;
		String key = "getActualKeyFrom"; //TODO: Get from external and secure key store, 16 bytes key
		byte[] cipherBlob = null;
		String out_data = null;
		
		try {			
			iv = new IvParameterSpec(initVector.getBytes("UTF-8"));
			skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
	        cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");				
			cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
			cipherBlob = cipher.update(data.getBytes("UTF-8"));
			out_data = new String(Base64.getEncoder().encode(cipherBlob));
			
		} catch (IOException | NoSuchAlgorithmException | NoSuchPaddingException | 
				InvalidKeyException | InvalidAlgorithmParameterException e) {
			ctx.log.emit("Encryption function - raised IOException: " + e.getMessage());
		}

		return out_data;
	}
	
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.log.emit("Init encryption Function");

		String data;
		
		while((data = ctx.object.stream.read()) != null) {
			data = encrypt(ctx, data);
			ctx.object.stream.write(data);
		}

		ctx.log.emit("Ended encryption Function");

	}
	
}
