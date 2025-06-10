import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";

export default function VeloxConBanner() {
  return (
    <section className={styles.banner}>
      <div className="container">
        <div className="row">
          <div className="col col--9">
            <h2>VeloxCon 2025 Sessions Are Now Available On-Demand</h2>
          </div>
          <div className="col col--3">
            <a
              className="button button--info button--lg"
              href="https://www.youtube.com/playlist?list=PLJvBe8nQAEsE0dT7XVIrD8QE-gmuX3Fe6"
              target="_blank"
              rel="noopener noreferrer"
            >
              Watch Now
            </a>
          </div>
        </div>
      </div>
    </section>
  );
}
