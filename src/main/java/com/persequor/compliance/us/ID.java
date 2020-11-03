package com.persequor.compliance.us;

public class ID {
	private byte[] bytes;

	private ID(byte[] bytes) {
		this.bytes = bytes;
	}

	public static ID of(byte[] bytes) {
		return new ID(bytes);
	}

	public byte[] getBytes() {
		return bytes;
	}
}
