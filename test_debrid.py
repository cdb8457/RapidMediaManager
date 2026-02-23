import os
import sys
import time

# Add the project root to sys.path so we can import our new module correctly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from media_manager.torrent.download_clients.real_debrid_api import RealDebridAPI

def main():
    print("--- REAL-DEBRID ISOLATED TEST SCRIPT ---")
    api_key = "WB33OMPGHRCBNW7JQXTFWNAFHIQB5KWDZZDHE5KQBOJPHB4MVOKQ"
    if not api_key:
        print("ERROR: Please set the RD_API_KEY environment variable.")
        print("Example (PowerShell): $env:RD_API_KEY=\"your_real_debrid_api_token\"")
        print("Example (CMD):        set RD_API_KEY=your_real_debrid_api_token")
        print("Then run:             python test_debrid.py")
        sys.exit(1)

    try:
        rd = RealDebridAPI(api_token=api_key)
        
        # We will use a known safe magnet link for testing: an Ubuntu desktop image
        # This is almost guaranteed to be instantly cached on Real-Debrid servers
        magnet_link = "magnet:?xt=urn:btih:3b245504cf5f11bbdbe1201cea6a6bf45aee1bc0&dn=ubuntu-24.04.1-desktop-amd64.iso"
        
        print(f"\n1. Submitting test magnet link (Ubuntu ISO)...")
        torrent_id = rd.add_magnet(magnet_link)
        print(f"   Success! Torrent ID: {torrent_id}")
        
        print("\n2. Getting torrent info to select files...")
        # Give RD's servers a quick second to parse the magnet
        time.sleep(1) 
        
        # Selecting 'all' files triggers RD to begin caching/preparing the download
        rd.select_files(torrent_id, "all")
        print("   Success! Selected all files.")
        
        print("\n3. Waiting for torrent to be cached (Ubuntu is usually instant)...")
        
        links = []
        attempts = 0
        while attempts < 15:
            info = rd.get_torrent_info(torrent_id)
            status = info.get("status")
            progress = info.get("progress", 0)
            
            print(f"   -> Status: {status} (Progress: {progress}%)")
            
            if status == "downloaded":
                links = info.get("links", [])
                break
            elif status == "waiting_files_selection":
                # Fallback purely for safety
                rd.select_files(torrent_id, "all")
                
            time.sleep(2)
            attempts += 1
            
        if not links:
            print("\nFAILURE: Did not receive download links from Debrid in time.")
            sys.exit(1)
            
        print(f"\n4. Found {len(links)} encrypted link(s). Unrestricting the first link...")
        unrestricted = rd.unrestrict_link(links[0])
        
        print("\n" + "="*50)
        print("🎉 TEST SUCCESS! THE DEBRID ENGINE WORKS.")
        print("="*50)
        print(f"Final Direct Download Link:")
        print(f"=> {unrestricted.get('download')}")
        print("\nYou can paste the link above into a browser to verify it downloads perfectly.")
        
        # Clean up so we don't pollute the user's real-debrid queue
        print(f"\n5. Cleaning up... deleting test torrent {torrent_id} from your Debrid account.")
        rd.delete_torrent(torrent_id)
        print("   Cleanup complete!")

    except Exception as e:
        print(f"\nTEST FAILED WITH ERROR: {e}")

if __name__ == "__main__":
    main()
